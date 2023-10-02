package ai.lzy.env.aux;

import ai.lzy.env.base.BaseEnvironment;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.annotation.Nullable;
import org.apache.commons.io.IOUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.yaml.snakeyaml.Yaml;

import java.io.IOException;
import java.nio.charset.Charset;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class CondaPackageRegistry {
    private static final Logger LOG = LogManager.getLogger(CondaPackageRegistry.class);
    private static final String VERSION_REGEX = "(?=>=)|(?=<=)|(?=<)|(?=>)|==|=";
    private static final int SPLIT_LIMIT = 2;
    private static final String DEFAULT_ENV_NAME = "base";
    private static final String DEFAULT_PYTHON_VERSION = "3.10";
    private static final String DEFAULT_PYPI_INDEX = "https://pypi.org/simple";

    // TODO(artolord) remove this ugly hack after removing conda.yaml
    private static final Map<String, String> NAME_TO_PYTHON_VERSION = Map.of(
        "py37", "3.7",
        "py38", "3.8",
        "py39", "3.9",
        "py310", "3.10",
        "py311", "3.11"
    );

    private final Map<String, CondaEnv> envs = new HashMap<>();
    private final BaseEnvironment baseEnv;
    private boolean inited = false;

    public CondaPackageRegistry(BaseEnvironment baseEnv) {
        this.baseEnv = baseEnv;
    }

    public void init() {
        if (inited) {
            return;
        }

        inited = true;

        final List<String> list;

        try {
            var envListJson = execInConda("conda env list --json");

            var mapper = new ObjectMapper();

            Map<String, List<String>> envList = mapper.readValue(envListJson, new TypeReference<>() {});
            list = envList.get("envs");
        } catch (Exception e) {
            LOG.warn("Cannot resolve conda envs in this environments", e);
            return;
        }

        if (list != null) {
            for (var env : list) {
                try {
                    var condaYaml = execInConda("conda activate %s && conda env export".formatted(env));
                    notifyInstalled(condaYaml);
                } catch (Exception e) {
                    LOG.error("Error while getting conda env spec of env {}: ", env, e);
                }
            }
        }
    }

    private record Package(String name, @Nullable String version) {
    }

    private record CondaEnv(
        String name,
        Map<String, Package> packages,
        String pythonVersion,
        String pypiIndex
    ) {}

    /**
     *  Builds actual conda yaml from env description.
     *  Returns null if it is no need for env installation
     *  Signature will be changed to {@code (packages, pythonVersion) -> condaYaml}
     */
    @Nullable
    public String buildCondaYaml(String condaYaml) {
        var env = build(condaYaml);

        if (env == null) {
            throw new IllegalArgumentException("Cannot build env from yaml");
        }

        return buildCondaYaml(env.packages, env.pythonVersion, env.pypiIndex);
    }

    @Nullable
    private String buildCondaYaml(Map<String, Package> packages, String pythonVersion, String pypiIndex) {
        try {
            var installedEnv = envs.values().stream()
                .filter(t -> t.pythonVersion.equals(pythonVersion))
                .findFirst()
                .orElse(null);

            if (installedEnv != null) {
                var failed = false;

                for (var pkg: packages.values()) {
                    var installedPkg = installedEnv.packages.get(pkg.name);

                    if (installedPkg == null) {
                        failed = true;
                        break;
                    }

                    if (pkg.version == null) {
                        continue;
                    }

                    if (!installedPkg.version.equals(pkg.version)) {
                        failed = true;
                        break;
                    }
                }

                if (!failed) {
                    return null;
                }

                return buildYaml(new CondaEnv(installedEnv.name, packages, installedEnv.pythonVersion, pypiIndex));
            }

        } catch (Exception e) {
            LOG.error("Error while building conda yaml for packages {}: ", packages, e);
        }

        return buildYaml(new CondaEnv("py" + pythonVersion, packages, pythonVersion, pypiIndex));
    }

    public void notifyInstalled(String condaYaml) {
        try {
            var env = build(condaYaml);
            if (env == null) {
                LOG.warn("Cannot save installed env {}", condaYaml);
                return;
            }

            envs.put(env.name, env);
        } catch (Exception e) {
            LOG.error("Cannot process condaYaml after installation {}: ", condaYaml, e);
        }
    }

    public String resolveEnvName(String condaYaml) {
        try {
            var env = build(condaYaml);
            if (env == null) {
                LOG.warn("Cannot process env {}", condaYaml);
                return DEFAULT_ENV_NAME;
            }

            var installedEnv = envs.values().stream()
                .filter(t -> t.pythonVersion.equals(env.pythonVersion))
                .findFirst()
                .orElse(null);

            if (installedEnv == null) {
                return "py" + env.pythonVersion;
            }

            return installedEnv.name;

        } catch (Exception e) {
            LOG.error("Cannot process condaYaml {}: ", condaYaml, e);
            return DEFAULT_ENV_NAME;
        }
    }

    @Nullable
    private CondaEnv build(String condaYaml) {
        var yaml = new Yaml();
        //noinspection unchecked
        var res = (Map<String, Object>) yaml.load(condaYaml);

        var name = (String) res.getOrDefault("name", "default");

        Map<String, Package> pkgs = new HashMap<>();

        //noinspection rawtypes
        var deps = (List) res.getOrDefault("dependencies", List.of());

        String pythonVersion = null;
        String pypiIndex = null;

        for (var dep : deps) {
            if (dep instanceof String) {
                var dat = ((String) dep).split(VERSION_REGEX, SPLIT_LIMIT);
                if (dat[0].equals("python")) {
                    pythonVersion = dat.length > 1 ? dat[1] : null;
                }

                //skipping other conda dependencies
            }

            //noinspection rawtypes
            if (dep instanceof Map depsMap) {
                var pipDeps = depsMap.get("pip");
                if (pipDeps == null) {
                    return null;
                }
                if (!(pipDeps instanceof List)) {
                    return null;
                }

                //noinspection unchecked
                for (var pipDep : (List<Object>) pipDeps) {
                    if (!(pipDep instanceof String)) {
                        return null;
                    }

                    if (((String) pipDep).startsWith("--index-url")) {
                        var parts = ((String) pipDep).split(" ");
                        pypiIndex = parts.length > 1 ? parts[1] : null;
                        continue;
                    }

                    var dat = ((String) pipDep).split(VERSION_REGEX, SPLIT_LIMIT);

                    var pkgName = normalizePkgName(dat[0]);
                    if (dat.length == 1) {
                        pkgs.put(pkgName, new Package(pkgName, null));
                    } else {
                        pkgs.put(pkgName, new Package(pkgName, dat[1]));
                    }
                }
            }
        }

        if (pythonVersion != null) {
            var splitPythonVersion = pythonVersion.split("\\.", 3);

            if (splitPythonVersion.length > 1) {

                // Looking only for minor python version
                pythonVersion = String.join(".", splitPythonVersion[0], splitPythonVersion[1]);
            } else {
                LOG.warn("Invalid python version {}, using default", pythonVersion);
                pythonVersion = DEFAULT_PYTHON_VERSION;
            }
        } else {
            pythonVersion = NAME_TO_PYTHON_VERSION.get(name);

            if (pythonVersion == null) {
                pythonVersion = DEFAULT_PYTHON_VERSION;
            }
        }

        return new CondaEnv(name, pkgs, pythonVersion, pypiIndex == null ? DEFAULT_PYPI_INDEX : pypiIndex);
    }

    private String buildYaml(CondaEnv env) {
        var cfg = Map.of(
            "name", env.name,
            "dependencies", List.of(
                "python=" + env.pythonVersion,
                "pip",
                Map.of("pip", env.packages.values().stream()
                    .map(p -> p.version != null ? p.name + "==" + p.version : p.name)
                    .toList()
                )
            )
        );

        var yaml = new Yaml();
        return yaml.dump(cfg);
    }

    private String normalizePkgName(String pkg) {
        return pkg.toLowerCase().replace("_", "-");
    }

    private String execInConda(String command) throws IOException {
        var proc = baseEnv.runProcess("bash", "-c", "eval \"$(conda shell.bash hook)\" && " + command);
        var out = IOUtils.toString(proc.out(), Charset.defaultCharset());
        var err = IOUtils.toString(proc.err(), Charset.defaultCharset());

        final int rc;
        try {
            rc = proc.waitFor();
        } catch (InterruptedException e) {
            throw new IOException("Interrupted while executing command %s in conda".formatted(command));
        }

        if (rc == 0) {
            return out;
        } else {
            throw new IOException("Error while executing command %s: rc=%d, error: %s".formatted(command, rc, err));
        }
    }
}
