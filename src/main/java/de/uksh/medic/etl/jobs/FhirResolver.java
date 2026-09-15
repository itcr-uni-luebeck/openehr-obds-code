package de.uksh.medic.etl.jobs;

import ca.uhn.fhir.context.FhirContext;
import ca.uhn.fhir.rest.client.api.IGenericClient;
import ca.uhn.fhir.rest.client.exceptions.FhirClientConnectionException;
import ca.uhn.fhir.rest.client.interceptor.AdditionalRequestHeadersInterceptor;
import ca.uhn.fhir.rest.server.exceptions.ResourceNotFoundException;
import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import de.uksh.medic.etl.ServerAvailability;
import de.uksh.medic.etl.settings.Settings;
import java.net.URI;
import java.util.concurrent.TimeUnit;
import org.hl7.fhir.r4.model.*;
import org.hl7.fhir.r4.model.Parameters.ParametersParameterComponent;
import org.tinylog.Logger;

/**
 * Class to query a FHIR server for data.
 */
public final class FhirResolver {

    private static final int CACHESIZE = Settings.getCacheSize();
    private static final FhirContext CTX = FhirContext.forR4();
    private static final Cache<String, Coding> CACHE_LOOKUP = Caffeine.newBuilder()
            .expireAfterAccess(24, TimeUnit.HOURS).maximumSize(CACHESIZE).build();
    private static final Cache<String, Coding> CACHE_CONCEPTMAP = Caffeine.newBuilder()
            .expireAfterAccess(24, TimeUnit.HOURS).maximumSize(CACHESIZE).build();
    private static final Cache<String, Boolean> CACHE_AVAILABLE = Caffeine.newBuilder()
            .expireAfterAccess(24, TimeUnit.HOURS).maximumSize(CACHESIZE).build();
    private static IGenericClient terminologyClient;

    /**
     * Initializes the class by creating clients for clinical data and terminology.
     */
    public FhirResolver() {
        if (Settings.getFhirTsUrl() != null) {
            String topicName = (Settings.getKafka() != null && Settings.getKafka().getReadTopic() != null
                    && !"".equals(Settings.getKafka().getReadTopic())) ? Settings.getKafka().getReadTopic()
                            : "JVM-ETL Dev";
            terminologyClient = CTX.newRestfulGenericClient(Settings.getFhirTsUrl().toString());
            AdditionalRequestHeadersInterceptor interceptor = new AdditionalRequestHeadersInterceptor();
            interceptor.addHeaderValue("User-Agent", topicName);
            terminologyClient.registerInterceptor(interceptor);
        }
    }

    /**
     * @return the FHIR terminology client, or null if not configured
     */
    public static IGenericClient getTerminologyClient() {
        return terminologyClient;
    }

    /**
     * Checks whether the given CodeSystem exists on the terminology server.
     * Positive results are cached in the shared CACHE_AVAILABLE, negative results are
     * deliberately never cached so that a missing CodeSystem is always checked freshly
     * against the server.
     *
     * Keys are "system|version". The "|" separator cannot appear unescaped in a URI, so
     * these keys can never collide with the ConceptMap keys stored in the same cache.
     *
     * @param system the CodeSystem URI
     * @param version the CodeSystem version (may be null)
     * @return TRUE if the CodeSystem exists, FALSE if it was not found, null if the
     *         terminology server could not be reached
     */
    private static Boolean codeSystemAvailable(URI system, String version) {
        String key = system + "|" + version;
        if (Boolean.TRUE.equals(CACHE_AVAILABLE.getIfPresent(key))) {
            return Boolean.TRUE;
        }

        try {
            Bundle bundle = terminologyClient.search()
                    .forResource(CodeSystem.class)
                    .where(CodeSystem.URL.matches().value(system.toString()))
                    .and(CodeSystem.VERSION.exactly().code(version))
                    .returnBundle(Bundle.class)
                    .execute();

            if (bundle.getEntry().isEmpty()) {
                CACHE_AVAILABLE.invalidate(key);
                Logger.error("CodeSystem {} version {} was not found.", system, version);
                return Boolean.FALSE;
            }
            CACHE_AVAILABLE.put(key, Boolean.TRUE);
            return Boolean.TRUE;
        } catch (FhirClientConnectionException e) {
            ServerAvailability.markFhirTsUnavailable();
            Logger.error("Could not connect to FHIR Terminology Server", e);
            return null;
        }
    }

    /**
     * Checks whether the given ConceptMap exists on the terminology server.
     * Positive results are cached in the shared CACHE_AVAILABLE, negative results are
     * deliberately never cached so that a missing ConceptMap is always checked freshly
     * against the server.
     *
     * Keys are the plain ConceptMap URI, which never contains the "|" separator used by the
     * CodeSystem keys in the same cache, so the two key spaces cannot collide.
     *
     * @param conceptMapUri the ConceptMap URL
     * @return TRUE if the ConceptMap exists, FALSE if it was not found, null if the
     *         terminology server could not be reached
     */
    private static Boolean conceptMapAvailable(URI conceptMapUri) {
        String key = conceptMapUri.toString();
        if (Boolean.TRUE.equals(CACHE_AVAILABLE.getIfPresent(key))) {
            return Boolean.TRUE;
        }

        try {
            Bundle bundle = terminologyClient.search()
                    .forResource(ConceptMap.class)
                    .where(ConceptMap.URL.matches().value(conceptMapUri.toString()))
                    .returnBundle(Bundle.class)
                    .execute();

            if (bundle.getEntry().isEmpty()) {
                CACHE_AVAILABLE.invalidate(key);
                Logger.error("ConceptMap {} was not found.", conceptMapUri);
                return Boolean.FALSE;
            }
            CACHE_AVAILABLE.put(key, Boolean.TRUE);
            return Boolean.TRUE;
        } catch (FhirClientConnectionException e) {
            ServerAvailability.markFhirTsUnavailable();
            Logger.error("Could not connect to FHIR Terminology Server", e);
            return null;
        }
    }

    public Coding conceptMap(URI conceptMapUri, URI system, URI source, URI target, String input) {
        String key = String.join("|", conceptMapUri.toString(), system.toString(), source.toString(), target.toString(),
                input);
        Coding c = CACHE_CONCEPTMAP.getIfPresent(key);
        if (c == null) {
            c = conceptMapServer(conceptMapUri, system, source, target, input);
            if (c != null) {
                CACHE_CONCEPTMAP.put(key, c);
            }
        }
        return c;
    }

    public Coding conceptMapServer(URI conceptMapUri, URI system, URI source, URI target, String input) {
        Parameters params = new Parameters();
        params.addParameter("url", new UriType(conceptMapUri));
        params.addParameter("system", new UriType(system));
        params.addParameter("source", new UriType(source));
        params.addParameter("target", new UriType(target));
        params.addParameter("code", new CodeType(input));

        Boolean available = conceptMapAvailable(conceptMapUri);
        if (available == null) {
            return null;
        }
        if (!available) {
            throw new ResourceNotFoundException("ConceptMap " + conceptMapUri + " was not found.");
        }

        try {
            Parameters result = terminologyClient.operation()
                    .onType("ConceptMap")
                    .named("translate").withParameters(params).useHttpGet().execute();

            for (ParametersParameterComponent p : result.getParameter()) {
                if (!p.getName().equals("match")) {
                    continue;
                }
                for (ParametersParameterComponent c : p.getPart()) {
                    if (c.getValue() instanceof Coding) {
                        return (Coding) c.getValue();
                    }
                }
            }

        } catch (FhirClientConnectionException e) {
            ServerAvailability.markFhirTsUnavailable();
            Logger.error("Could not connect to FHIR Terminology Server", e);
        }
        Logger.warn("Could not map code {} from {} to {}", input, source, target);
        return null;
    }

    public Coding lookUp(URI system, String version, String code) {
        String key = String.join("|", system.toString(), version, code);
        Coding c = CACHE_LOOKUP.getIfPresent(key);
        if (c == null) {
            c = lookUpServer(system, version, code);
            if (c != null) {
                CACHE_LOOKUP.put(key, c);
            }
        }
        return c;
    }

    public Coding lookUpServer(URI system, String version, String code) {
        Parameters params = new Parameters();
        params.addParameter("system", new UriType(system));
        params.addParameter("code", code);
        params.addParameter("version", version);

        Boolean available = codeSystemAvailable(system, version);
        if (available == null) {
            return null;
        }
        if (!available) {
            throw new ResourceNotFoundException("CodeSystem " + system + " version " + version + " was not found.");
        }

        try {
            Parameters result = terminologyClient.operation().onType(CodeSystem.class)
                    .named("lookup").withParameters(params).useHttpGet().execute();

            Coding coding = new Coding();
            for (ParametersParameterComponent p : result.getParameter()) {
                switch (p.getName()) {
                    case "display" -> coding.setDisplayElement((StringType) p.getValue());
                    case "version" -> coding.setVersionElement((StringType) p.getValue());
                    case "system" -> coding.setSystemElement((UriType) p.getValue());
                    case "code" -> coding.setCodeElement((CodeType) p.getValue());
                    default -> {
                    }
                }
            }

            if (version == null) {
                return coding.setVersion(null);
            }

            return coding;

        } catch (FhirClientConnectionException e) {
            ServerAvailability.markFhirTsUnavailable();
            Logger.error("Could not connect to FHIR Terminology Server", e);
        } catch (ResourceNotFoundException e) {
            Logger.warn("Code " + code + " was not found in CodeSystem " + system + ".", e);
        }
        return null;
    }

    public Coding lookUpDesignation(URI system, String version, String code, String designation) {
        String key = String.join("|", system.toString(), version, code, designation);
        Coding c = CACHE_LOOKUP.getIfPresent(key);
        if (c == null) {
            c = lookUpServerDesignation(system, version, code, designation);
            if (c != null) {
                CACHE_LOOKUP.put(key, c);
            }
        }
        return c;
    }

    public Coding lookUpServerDesignation(URI system, String version, String code, String designation) {
        Parameters params = new Parameters();
        params.addParameter("system", new UriType(system));
        params.addParameter("code", code);
        params.addParameter("version", version);

        Boolean available = codeSystemAvailable(system, version);
        if (available == null) {
            return null;
        }
        if (!available) {
            throw new ResourceNotFoundException("CodeSystem " + system + " version " + version + " was not found.");
        }

        try {
            Parameters result = terminologyClient.operation().onType(CodeSystem.class)
                    .named("lookup").withParameters(params).useHttpGet().execute();

            Coding coding = new Coding();
            String designatedDisplay = null;
            for (ParametersParameterComponent p : result.getParameter()) {
                switch (p.getName()) {
                    case "display" -> coding.setDisplayElement((StringType) p.getValue());
                    case "version" -> coding.setVersionElement((StringType) p.getValue());
                    case "system" -> coding.setSystemElement((UriType) p.getValue());
                    case "code" -> coding.setCodeElement((CodeType) p.getValue());
                    case "designation" -> {
                        String value = null;
                        boolean use = false;
                        boolean language = false;
                        for (ParametersParameterComponent pcp : p.getPart()) {
                            switch (pcp.getName()) {
                                case "language" ->
                                    language = designation.equals(((StringType) pcp.getValue()).getValueAsString());
                                case "use" -> use = "preferredForLanguage".equals(((Coding) pcp.getValue()).getCode());
                                case "value" -> value = ((StringType) pcp.getValue()).getValueAsString();
                                default -> {
                                }
                            }
                        }
                        if (language && use && value != null) {
                            designatedDisplay = value;
                        }
                    }
                    default -> {
                    }
                }
            }

            if (designatedDisplay != null) {
                coding.setDisplay(designatedDisplay);
            }
            if (version == null) {
                return coding.setVersion(null);
            }

            return coding;

        } catch (FhirClientConnectionException e) {
            ServerAvailability.markFhirTsUnavailable();
            Logger.error("Could not connect to FHIR Terminology Server", e);
        } catch (ResourceNotFoundException e) {
            Logger.warn("Code " + code + " was not found in CodeSystem " + system + ".", e);
        }

        return null;
    }

    public String getCacheSize() {
        return "{\"lookup\": " + CACHE_LOOKUP.estimatedSize() + ", \"conceptMap\": " + CACHE_CONCEPTMAP.estimatedSize()
                + "}";
    }

}
