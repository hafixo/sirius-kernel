/*
 * Made with all the love in the world
 * by scireum in Remshalden, Germany
 *
 * Copyright by scireum GmbH
 * http://www.scireum.de - info@scireum.de
 */

package sirius.kernel;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import org.apache.log4j.Level;
import sirius.kernel.async.Future;
import sirius.kernel.async.Operation;
import sirius.kernel.async.Tasks;
import sirius.kernel.commons.Strings;
import sirius.kernel.commons.Value;
import sirius.kernel.commons.Watch;
import sirius.kernel.di.Injector;
import sirius.kernel.di.std.Part;
import sirius.kernel.di.std.PriorityParts;
import sirius.kernel.health.Exceptions;
import sirius.kernel.health.Log;
import sirius.kernel.nls.NLS;
import sirius.kernel.settings.ExtendedSettings;

import javax.annotation.Nullable;
import java.lang.management.ManagementFactory;
import java.lang.management.ThreadInfo;
import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

/**
 * Loads and initializes the framework.
 * <p>
 * This can be considered the <tt>stage2</tt> when booting the framework, as it is responsible to discover and
 * initialize all components. Call {@link #start(Setup)} to initialize the framework.
 * <p>
 * To make a jar or other classpath-root visible to SIRIUS an empty file called "component.marker" must be placed in
 * its root directory.
 */
public class Sirius {

    private static final String CONFIG_KEY_CUSTOMIZATIONS = "sirius.customizations";
    private static final String SEPARATOR_LINE = "---------------------------------------------------------";
    private static Setup setup;
    private static Config config;
    private static ExtendedSettings settings;
    private static Map<String, Boolean> frameworks = Maps.newHashMap();
    private static List<String> customizations = Lists.newArrayList();
    private static Classpath classpath;
    private static volatile boolean started = false;
    private static volatile boolean initialized = false;
    private static final long START_TIMESTAMP = System.currentTimeMillis();

    protected static final Log LOG = Log.get("sirius");

    private static final String DEBUG_LOGGER_NAME = "debug";

    /**
     * This debug logger will be logging all messages when {@link sirius.kernel.Sirius#isDev()} is true. Otherwise,
     * this logger is set to "OFF".
     */
    public static final Log DEBUG = Log.get(DEBUG_LOGGER_NAME);

    @PriorityParts(Lifecycle.class)
    private static List<Lifecycle> lifecycleParticipants;

    @Part
    private static Tasks tasks;

    private Sirius() {
    }

    /**
     * Determines if the framework is running in development or in production mode.
     *
     * @return {@code true} is the framework runs in development mode, false otherwise.
     */
    public static boolean isDev() {
        return setup.getMode() == Setup.Mode.DEV;
    }

    /**
     * Determines if the framework was started as test run (JUNIT or the like).
     *
     * @return <tt>true</tt> if the framework was started as test, <tt>false</tt> otherwise
     */
    public static boolean isStartedAsTest() {
        return setup != null && setup.getMode() == Setup.Mode.TEST;
    }

    /**
     * Determines if the framework is running in development or in production mode.
     *
     * @return {@code true} is the framework runs in production mode, false otherwise.
     */
    public static boolean isProd() {
        return !isDev();
    }

    /*
     * Once the configuration is loaded, this method applies the log level to all log4j and java.util.logging
     * loggers
     */
    private static void setupLogLevels() {
        if (Sirius.isDev()) {
            Log.setLevel(DEBUG_LOGGER_NAME, Level.ALL);
        } else {
            Log.setLevel(DEBUG_LOGGER_NAME, Level.OFF);
        }
        if (!config.hasPath("logging")) {
            return;
        }
        LOG.INFO("Initializing the log system:");
        Config logging = config.getConfig("logging");
        for (Map.Entry<String, com.typesafe.config.ConfigValue> entry : logging.entrySet()) {
            LOG.INFO("* Setting %s to: %s", entry.getKey(), logging.getString(entry.getKey()));
            Log.setLevel(entry.getKey(), Level.toLevel(logging.getString(entry.getKey())));
        }
    }

    /*
     * Scans the system config (sirius.frameworks) and determines which frameworks are enabled. This will affect
     * which classes are loaded into the component model.
     */
    private static void setupFrameworks() {
        Config frameworkConfig = config.getConfig("sirius.frameworks");
        Map<String, Boolean> frameworkStatus = Maps.newHashMap();
        int total = 0;
        int numEnabled = 0;
        LOG.DEBUG_INFO("Scanning framework status (sirius.frameworks):");
        for (Map.Entry<String, com.typesafe.config.ConfigValue> entry : frameworkConfig.entrySet()) {
            String framework = entry.getKey();
            try {
                boolean enabled = Value.of(entry.getValue().unwrapped()).asBoolean(false);
                frameworkStatus.put(framework, enabled);
                total++;
                numEnabled += enabled ? 1 : 0;
                LOG.DEBUG_INFO(Strings.apply("  * %s: %b", framework, enabled));
            } catch (Exception e) {
                Exceptions.ignore(e);
                LOG.WARN("Cannot convert status '%s' of framework '%s' to a boolean! Framework will be disabled.",
                         entry.getValue().render(),
                         framework);
                frameworkStatus.put(framework, false);
            }
        }
        LOG.INFO("Enabled %d of %d frameworks...", numEnabled, total);
        // Although customizations are loaded in setupConfiguration, we output the status here,
        // as this seems more intiutive for the customer (the poor guy reading the logs...)
        LOG.INFO("Active Customizations: %s", customizations);

        frameworks = frameworkStatus;
    }

    /*
     * Starts all framework components
     */
    private static void startComponents() {
        if (started) {
            stop();
        }
        started = true;
        boolean startFailed = false;
        for (final Lifecycle lifecycle : lifecycleParticipants) {
            Future future = tasks.defaultExecutor().fork(() -> startLifecycle(lifecycle));
            if (!future.await(Duration.ofMinutes(1))) {
                LOG.WARN("Lifecycle '%s' (%s) did not start within one minute....",
                         lifecycle,
                         lifecycle.getClass().getName());
                startFailed = true;
            }
        }

        if (startFailed) {
            outputActiveOperations();
        }
    }

    private static void startLifecycle(Lifecycle lifecycle) {
        LOG.INFO("Starting: %s", lifecycle.getName());
        try {
            lifecycle.started();
        } catch (Exception e) {
            Exceptions.handle()
                      .error(e)
                      .to(LOG)
                      .withSystemErrorMessage("Startup of: %s failed!", lifecycle.getName())
                      .handle();
        }
    }

    /**
     * Discovers all components in the class path and initializes the {@link Injector}
     */
    private static void init() {
        if (initialized) {
            return;
        }
        initialized = true;
        classpath = new Classpath(setup.getLoader(), "component.marker", customizations);

        if (isStartedAsTest()) {
            // Load test configurations (will override component configs)
            classpath.find(Pattern.compile("component-test-([^\\-]*?)\\.conf")).forEach(value -> {
                try {
                    config = config.withFallback(ConfigFactory.load(setup.getLoader(), value.group()));
                } catch (Exception e) {
                    handleConfigError(value.group(), e);
                }
            });
        }

        // Load component configurations
        classpath.find(Pattern.compile("component-([^\\-]*?)\\.conf")).forEach(value -> {
            if (!"test".equals(value.group(1))) {
                try {
                    config = config.withFallback(ConfigFactory.load(setup.getLoader(), value.group()));
                } catch (Exception e) {
                    handleConfigError(value.group(), e);
                }
            }
        });

        // Setup log-system based on configuration
        setupLogLevels();

        // Output enabled frameworks...
        setupFrameworks();

        // Setup native language support
        NLS.init(classpath);

        // Initialize dependency injection...
        Injector.init(classpath);

        startComponents();

        // Start resource monitoring...
        NLS.startMonitoring(classpath);
    }

    private static void handleConfigError(String file, Exception e) {
        Exceptions.ignore(e);
        Sirius.LOG.WARN("Cannot load %s: %s", file, e.getMessage());
    }

    /**
     * Provides access to the classpath used to load the framework.
     *
     * @return the classpath used to load the framework
     */
    public static Classpath getClasspath() {
        return classpath;
    }

    /**
     * Stops the framework.
     */
    public static void stop() {
        if (!started) {
            return;
        }
        LOG.INFO("Stopping Sirius");
        LOG.INFO(SEPARATOR_LINE);
        outputActiveOperations();
        stopLifecycleParticipants();
        outputActiveOperations();
        waitForLifecyclePaticipants();
        outputThreadState();
        started = false;
    }

    private static void outputThreadState() {
        LOG.INFO("System halted! - Thread State");
        LOG.INFO(SEPARATOR_LINE);
        LOG.INFO("%-15s %10s %53s", "STATE", "ID", "NAME");
        for (ThreadInfo info : ManagementFactory.getThreadMXBean().dumpAllThreads(false, false)) {
            LOG.INFO("%-15s %10s %53s", info.getThreadState().name(), info.getThreadId(), info.getThreadName());
        }
        LOG.INFO(SEPARATOR_LINE);
    }

    private static void waitForLifecyclePaticipants() {
        LOG.INFO("Awaiting system halt...");
        LOG.INFO(SEPARATOR_LINE);
        for (int i = lifecycleParticipants.size() - 1; i >= 0; i--) {
            Lifecycle lifecycle = lifecycleParticipants.get(i);
            try {
                Watch w = Watch.start();
                lifecycle.awaitTermination();
                LOG.INFO("Terminated: %s (Took: %s)", lifecycle.getName(), w.duration());
            } catch (Exception e) {
                Exceptions.handle()
                          .error(e)
                          .to(LOG)
                          .withSystemErrorMessage("Termination of: %s failed!", lifecycle.getName())
                          .handle();
            }
        }
    }

    private static void stopLifecycleParticipants() {
        LOG.INFO("Stopping lifecycles...");
        LOG.INFO(SEPARATOR_LINE);
        for (int i = lifecycleParticipants.size() - 1; i >= 0; i--) {
            Lifecycle lifecycle = lifecycleParticipants.get(i);
            Future future = tasks.defaultExecutor().fork(() -> stopLifecycle(lifecycle));
            if (!future.await(Duration.ofSeconds(10))) {
                LOG.WARN("Lifecycle '%s' (%s) did not stop within 10 seconds....",
                         lifecycle,
                         lifecycle.getClass().getName());
            }
        }
        LOG.INFO(SEPARATOR_LINE);
    }

    private static void stopLifecycle(Lifecycle lifecycle) {
        LOG.INFO("Stopping: %s", lifecycle.getName());
        try {
            lifecycle.stopped();
        } catch (Exception e) {
            Exceptions.handle()
                      .error(e)
                      .to(LOG)
                      .withSystemErrorMessage("Stop of: %s failed!", lifecycle.getName())
                      .handle();
        }
    }

    private static void outputActiveOperations() {
        if (!Operation.getActiveOperations().isEmpty()) {
            LOG.INFO("Active Operations");
            LOG.INFO(SEPARATOR_LINE);
            for (Operation op : Operation.getActiveOperations()) {
                LOG.INFO(op.toString());
            }
            LOG.INFO(SEPARATOR_LINE);
        }
    }

    /**
     * Initializes the framework.
     * <p>
     * This is called by <tt>IPL.main</tt> once the class loader is fully populated.
     *
     * @param setup the setup class used to configure the framework
     */
    public static void start(Setup setup) {
        Watch w = Watch.start();
        Sirius.setup = setup;
        setup.init();
        LOG.INFO(SEPARATOR_LINE);
        LOG.INFO("System is STARTING...");
        LOG.INFO(SEPARATOR_LINE);
        LOG.INFO("Loading config...");
        LOG.INFO(SEPARATOR_LINE);
        setupConfiguration();
        LOG.INFO(SEPARATOR_LINE);
        LOG.INFO("Starting the system...");
        LOG.INFO(SEPARATOR_LINE);
        init();
        LOG.INFO(SEPARATOR_LINE);
        LOG.INFO("System is UP and RUNNING - %s", w.duration());
        LOG.INFO(SEPARATOR_LINE);

        Runtime.getRuntime().addShutdownHook(new Thread(Sirius::stop));
    }

    /**
     * Determines if the framework with the given name is enabled.
     * <p>
     * Frameworks can be enabled or disabled using the config path <tt>sirius.framework.[name]</tt>. This is
     * intensively used by the app part, as it provides a lot of basic frameworks which can be turned off or
     * on as required.
     *
     * @param framework the framework to check
     * @return <tt>true</tt> if the framework is enabled, <tt>false</tt> otherwise
     */
    public static boolean isFrameworkEnabled(String framework) {
        if (Strings.isEmpty(framework)) {
            return true;
        }
        if (Sirius.isDev() && !frameworks.containsKey(framework)) {
            LOG.WARN("Status of unknown framework '%s' requested. Will report as disabled framework.", framework);
        }
        return Boolean.TRUE.equals(frameworks.get(framework));
    }

    /**
     * Returns a list of all active customer configurations.
     * <p>
     * A customer configuration can be used to override basic functionality with more specialized classes or resources
     * which were adapted based on customer needs.
     * <p>
     * As often groups of customers share the same requirements, not only a single configuration can be activated
     * but a list. Within this list each configuration may override classes and resources of all former
     * configurations. Therefore the last configuration will always "win".
     * <p>
     * Note that classes must be placed in the package: <b>configuration.[name]</b> (with arbitrary sub packages).
     * Also resources must be placed in: <b>configuration/[name]/resource-path</b>.
     *
     * @return a list of all active configurations
     */
    public static List<String> getActiveConfigurations() {
        return Collections.unmodifiableList(customizations);
    }

    /**
     * Determines if the given config is active (or null which is considered active)
     *
     * @param configName the name of the config to check. <tt>null</tt> will be considered as active
     * @return <tt>true</tt> if the named customization is active, <tt>false</tt> otherwise
     */
    public static boolean isActiveCustomization(@Nullable String configName) {
        return configName == null || Sirius.getActiveConfigurations().contains(configName);
    }

    /**
     * Determines if the given resource is part of a configuration.
     *
     * @param resource the resource path to check
     * @return <tt>true</tt> if the given resource is part of a configuration, <tt>false</tt> otherwise
     */
    public static boolean isCustomizationResource(@Nullable String resource) {
        return resource != null && resource.startsWith("customizations");
    }

    /**
     * Extracts the customization name from a resource.
     * <p>
     * Valid names are paths like "customizations/[name]/..." or classes like "customizations.[name]...".
     *
     * @param resource the name of the resource
     * @return the name of the customizations or <tt>null</tt> if no config name is contained
     */
    @Nullable
    public static String getCustomizationName(@Nullable String resource) {
        if (resource == null) {
            return null;
        }
        if (resource.startsWith("customizations/")) {
            return Strings.split(resource.substring(15), "/").getFirst();
        } else if (resource.startsWith("customizations.")) {
            return Strings.split(resource.substring(15), ".").getFirst();
        } else {
            return null;
        }
    }

    /**
     * Compares the two given customizations according to the order given in the system config.
     *
     * @param configA the name of the first customization
     * @param configB the name of the second customization
     * @return an int value which can be used to compare the order of the two given configs.
     */
    public static int compareCustomizations(@Nullable String configA, @Nullable String configB) {
        if (configA == null) {
            if (configB == null) {
                return 0;
            }
            return 1;
        }
        if (configB == null) {
            return -1;
        }
        return customizations.indexOf(configA) - customizations.indexOf(configB);
    }

    /*
     * Loads all relevant .conf files
     */
    private static void setupConfiguration() {
        config = setup.loadApplicationConfig();
        Config instanceConfig = null;
        if (isStartedAsTest()) {
            config = setup.applyTestConfig(config);
        } else {
            // instance.conf and develop.conf are not used to tests to permit uniform behaviour on local
            // machines and build servers...
            if (Sirius.isDev()) {
                config = setup.applyDeveloperConfig(config);
            }
            instanceConfig = setup.loadInstanceConfig();
        }

        // Setup customer customizations...
        if (instanceConfig != null && instanceConfig.hasPath(CONFIG_KEY_CUSTOMIZATIONS)) {
            customizations = instanceConfig.getStringList(CONFIG_KEY_CUSTOMIZATIONS);
        } else if (config.hasPath(CONFIG_KEY_CUSTOMIZATIONS)) {
            customizations = config.getStringList(CONFIG_KEY_CUSTOMIZATIONS);
        }

        // Load settings.conf for customizations...
        for (String conf : customizations) {
            if (Sirius.class.getResource("/customizations/" + conf + "/settings.conf") != null) {
                LOG.INFO("loading settings.conf for customization '" + conf + "'");
                String configName = "customizations/" + conf + "/settings.conf";
                try {
                    config = ConfigFactory.load(setup.getLoader(), configName).withFallback(config);
                } catch (Exception e) {
                    handleConfigError(configName, e);
                }
            } else {
                LOG.INFO("customization '" + conf + "' has no settings.conf...");
            }
        }

        // Apply instance config at last for override all other configs...
        if (instanceConfig != null) {
            config = instanceConfig.withFallback(config);
        }
    }

    /**
     * Returns the system config based on the current instance.conf (file system), application.conf (classpath) and
     * all component-XXX.conf wrapped as <tt>Settings</tt>
     *
     * @return the initialized settings object or <tt>null</tt> if the framework is not setup yet.
     */
    public static ExtendedSettings getSettings() {
        if (settings == null) {
            if (config != null) {
                settings = new ExtendedSettings(config);
            }
        }
        return settings;
    }

    /**
     * Provides access to the setup instance which was used to configure Sirius.
     *
     * @return the setup instance used to configure Sirius.+
     */
    public static Setup getSetup() {
        return setup;
    }

    /**
     * Returns the up time of the system in milliseconds.
     *
     * @return the number of milliseconds the system is running
     */
    public static long getUptimeInMilliseconds() {
        return System.currentTimeMillis() - START_TIMESTAMP;
    }
}
