package ai.lzy.env.aux;

import org.apache.commons.io.IOUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.yaml.snakeyaml.Yaml;

import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class CondaPackageRegistry {
    private static final Logger LOG = LogManager.getLogger(CondaPackageRegistry.class);
    private static final String VERSION_REGEX = "(?=>=)|(?=<=)|(?=<)|(?=>)|==|=";
    private static final int SPLIT_LIMIT = 2;

    private final Map<String, Map<String, Package>> envs = new HashMap<>();

    public CondaPackageRegistry() {
        var list = System.getenv("LZY_CONDA_ENVS_LIST");
        if (list != null) {
            var envs = list.split(",");
            for (var env : envs) {
                try {
                    var process = Runtime.getRuntime().exec(new String[] {"conda", "env", "export", "-n", env});
                    String condaYaml = IOUtils.toString(process.getInputStream(), StandardCharsets.UTF_8);
                    notifyInstalled(condaYaml);
                    process.waitFor();
                } catch (Exception e) {
                    LOG.error("Error while getting conda env spec of env {}: ", env, e);
                }
            }
        }
    }

    private record Package(String name, String version) {
    }

    public boolean isInstalled(String condaYaml) {
        try {
            var yaml = new Yaml();
            Map<String, Object> conf = yaml.load(condaYaml);
            var name = (String) conf.getOrDefault("name", "default");

            if (!envs.containsKey(name)) {
                return false;
            }

            var env = envs.get(name);
            //noinspection unchecked
            var deps = (List<Object>) conf.getOrDefault("dependencies", List.of());

            for (var dep : deps) {

                if (!(dep instanceof String) && !(dep instanceof Map)) {
                    return false;
                }

                if (dep instanceof String s) {
                    var res = s.split(VERSION_REGEX, SPLIT_LIMIT);
                    if (res.length < 2) {
                        if (env.containsKey(res[0])) {
                            continue;
                        }
                        return false;
                    }
                    var packageName = res[0].strip();
                    var version = res[1].strip();

                    var pkg = env.get(packageName);

                    if (pkg == null) {
                        return false;
                    }

                    if (!pkg.version.equals(version)) {
                        return false;
                    }
                }

                //noinspection rawtypes
                if (dep instanceof Map depsMap) {
                    var pipDeps = depsMap.get("pip");
                    if (pipDeps == null) {
                        return false;
                    }
                    if (!(pipDeps instanceof List)) {
                        return false;
                    }

                    //noinspection unchecked
                    for (var pipDep : (List<Object>) pipDeps) {
                        if (!(pipDep instanceof String)) {
                            return false;
                        }

                        var res = ((String) pipDep).split(VERSION_REGEX, SPLIT_LIMIT);
                        if (res.length < 2) {
                            if (!env.containsKey(pipDep)) {
                                return false;
                            }
                            continue;
                        }

                        var pkgName = res[0].strip();
                        var version = res[1].strip();

                        if (!env.containsKey(pkgName)) {
                            return false;
                        }

                        if (!env.get(pkgName).version.equals(version)) {
                            return false;
                        }
                    }
                }
            }
            return true;
        } catch (Exception e) {
            LOG.error("Error while checking conda yaml {}: ", condaYaml, e);
            return false;
        }
    }

    public void notifyInstalled(String condaYaml) {
        try {
            var yaml = new Yaml();
            //noinspection unchecked
            var res = (Map<String, Object>) yaml.load(condaYaml);

            var name = (String) res.getOrDefault("name", "default");

            Map<String, Package> pkgs = new HashMap<>();

            //noinspection rawtypes
            var deps = (List) res.getOrDefault("dependencies", List.of());

            for (var dep : deps) {
                if (dep instanceof String) {
                    var dat = ((String) dep).split(VERSION_REGEX, SPLIT_LIMIT);
                    if (dat.length == 1) {
                        pkgs.put(dat[0], new Package(dat[0], "some-very-strange-version"));
                    } else {
                        pkgs.put(dat[0], new Package(dat[0], dat[1]));
                    }
                }

                //noinspection rawtypes
                if (dep instanceof Map depsMap) {
                    var pipDeps = depsMap.get("pip");
                    if (pipDeps == null) {
                        return;
                    }
                    if (!(pipDeps instanceof List)) {
                        return;
                    }

                    //noinspection unchecked
                    for (var pipDep : (List<Object>) pipDeps) {
                        if (!(pipDep instanceof String)) {
                            return;
                        }
                        var dat = ((String) pipDep).split(VERSION_REGEX, SPLIT_LIMIT);
                        if (dat.length == 1) {
                            pkgs.put(dat[0], new Package(dat[0], "some-very-strange-version"));
                        } else {
                            pkgs.put(dat[0], new Package(dat[0], dat[1]));
                        }
                    }
                }
            }

            envs.put(name, pkgs);

        } catch (Exception e) {
            LOG.error("Cannot process condaYaml after installation {}: ", condaYaml, e);
        }
    }
}
