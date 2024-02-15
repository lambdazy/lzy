package ai.lzy.env;


import ai.lzy.env.aux.CondaPackageRegistry;
import ai.lzy.env.base.ProcessEnvironment;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.regex.Pattern;

public class CondaPackageRegistryTest {

    private final CondaPackageRegistry condaPackageRegistry = new CondaPackageRegistry(new ProcessEnvironment());

    @Before
    public void before() {
        condaPackageRegistry.notifyInstalled("""
            name: default
            dependencies:
            - python==3.9.15
            - pip
            - cloudpickle=1.0.0
            - pip:
              - numpy
              - pylzy==1.0.0
              - serialzy>=1.0.0""");
    }

    @Test
    public void testSame() {
        Assert.assertNull(condaPackageRegistry.buildCondaYaml("""
            name: default
            dependencies:
            - python=3.9.15
            - pip
            - cloudpickle=1.0.0
            - pip:
              - numpy
              - pylzy==1.0.0
              - serialzy>=1.0.0"""));
    }

    @Test
    public void testSameUnspecifiedVersion() {
        Assert.assertNull(condaPackageRegistry.buildCondaYaml("""
            name: default
            dependencies:
            - python=3.9.15
            - pip
            - cloudpickle=1.0.0
            - pip:
              - numpy
              - pylzy==1.0.0
              - serialzy"""));
    }

    @Test
    public void testSameDifferentOrder() {
        Assert.assertNull(condaPackageRegistry.buildCondaYaml("""
            name: default
            dependencies:
            - cloudpickle=1.0.0
            - pip
            - python=3.9.15
            - pip:
              - numpy
              - serialzy>=1.0.0
              - pylzy==1.0.0"""));
    }

    @Test
    public void testLibEquality() {
        // currently we do not support inequalities parsing
        Assert.assertNotNull(condaPackageRegistry.buildCondaYaml("""
            name: default
            dependencies:
            - python=3.9.15
            - pip
            - cloudpickle=1.0.0
            - pip:
              - numpy
              - pylzy==1.0.0
              - serialzy==1.0.0"""));
    }

    @Test
    public void testLessLibs() {
        Assert.assertNull(condaPackageRegistry.buildCondaYaml("""
            name: default
            dependencies:
            - python=3.9.15
            - pip
            - cloudpickle=1.0.0
            - pip:
              - numpy
              - pylzy==1.0.0"""));
    }

    @Test
    public void testNoPipDependencies() {
        Assert.assertNull(condaPackageRegistry.buildCondaYaml("""
            name: default
            dependencies:
            - python=3.9.15
            - pip
            - cloudpickle=1.0.0"""));
    }

    @Test
    public void testNoPip() {
        Assert.assertNull(condaPackageRegistry.buildCondaYaml("""
            name: default
            dependencies:
            - python==3.9.15
            - cloudpickle=1.0.0
            - pip:
              - numpy
              - pylzy==1.0.0
              - serialzy>=1.0.0"""));
    }

    @Test
    public void testExtraLib() {
        Assert.assertNotNull(condaPackageRegistry.buildCondaYaml("""
            name: default
            dependencies:
            - python==3.9.15
            - pip
            - cloudpickle=1.0.0
            - pip:
              - numpy
              - pylzy==1.0.0
              - serialzy>=1.0.0
              - psutil==5.0.0"""));
    }

    @Test
    public void testOtherPythonVersion() {
        Assert.assertNotNull(condaPackageRegistry.buildCondaYaml("""
            name: default
            dependencies:
            - python=3.9.16
            - pip
            - cloudpickle=1.0.0
            - pip:
              - numpy
              - pylzy==1.0.0
              - serialzy==1.0.0"""));
    }

    @Test
    public void testPythonVersionInequality() {
        // currently we do not support inequalities parsing
        Assert.assertNotNull(condaPackageRegistry.buildCondaYaml("""
            name: default
            dependencies:
            - python>=3.9.15
            - pip
            - cloudpickle=1.0.0
            - pip:
              - numpy
              - pylzy==1.0.0
              - serialzy>=1.0.0"""));
    }

    @Test
    public void testSameAnotherName() {
        Assert.assertNull(condaPackageRegistry.buildCondaYaml("""
            name: default1
            dependencies:
            - python=3.9.15
            - pip
            - cloudpickle=1.0.0
            - pip:
              - numpy
              - pylzy==1.0.0
              - serialzy>=1.0.0"""));
    }

    @Test
    public void testInvalidPipDependenciesName() {
        Assert.assertThrows(IllegalArgumentException.class, () -> condaPackageRegistry.buildCondaYaml("""
            name: default
            dependencies:
            - python=3.9.15
            - pip
            - cloudpickle=1.0.0
            - pip2:
              - numpy
              - pylzy==1.0.0
              - serialzy>=1.0.0"""));
    }

    @Test
    public void testPipDependenciesNotList() {
        Assert.assertThrows(IllegalArgumentException.class, () -> condaPackageRegistry.buildCondaYaml("""
            name: default
            dependencies:
            - python=3.9.15
            - pip
            - cloudpickle=1.0.0
            - pip: pep"""));
    }

    @Test
    public void testPipDependenciesIsList() {
        Assert.assertThrows(IllegalArgumentException.class, () -> condaPackageRegistry.buildCondaYaml("""
            name: default
            dependencies:
            - python=3.9.15
            - pip
            - cloudpickle=1.0.0
            - pip:
              - pylzy:
                - lzy"""));
    }

    @Test
    public void testPipDependenciesVersionUpdate() {
        Assert.assertNotNull(condaPackageRegistry.buildCondaYaml("""
            name: default
            dependencies:
            - python=3.9.15
            - pip
            - cloudpickle=1.0.0
            - pip:
              - numpy
              - pylzy==1.0.0
              - serialzy>=2.0.0"""));
    }

    @Test
    public void testPipDependenciesNewDepWithoutVersion() {
        Assert.assertNotNull(condaPackageRegistry.buildCondaYaml("""
            name: default
            dependencies:
            - python=3.9.15
            - pip
            - cloudpickle=1.0.0
            - pip:
              - numpy
              - scipy
              - pylzy==1.0.0
              - serialzy>=1.0.0"""));
    }

    @Test
    public void testPipWithIndexUrl() {
        String condaYaml = condaPackageRegistry.buildCondaYaml("""
                name: default
                dependencies:
                - python=3.9.15
                - pip
                - cloudpickle=1.0.0
                - pip:
                  - numpy
                  - --index-url   https://pypi.ngc.nvidia.com
                  - scipy
                  - pylzy==1.0.0
                  - serialzy>=1.0.0""");
        Assert.assertNotNull(condaYaml);
        Assert.assertTrue(
                "Index url is not in conda yaml",
                condaYaml.contains("--index-url https://pypi.ngc.nvidia.com"));
    }

    @Test
    public void testPipWithoutExplicitIndexUrl() {
        String condaYaml = condaPackageRegistry.buildCondaYaml("""
                name: default
                dependencies:
                - python=3.9.15
                - pip
                - cloudpickle=1.0.0
                - pip:
                  - numpy
                  - scipy
                  - pylzy==1.0.0
                  - serialzy>=1.0.0""");
        Assert.assertNotNull(condaYaml);
        Assert.assertTrue(
                "Index url is not in conda yaml",
                condaYaml.contains("--index-url https://pypi.org/simple"));
    }

    @Test
    public void testPipWithExtraIndexUrls() {
        String condaYaml = condaPackageRegistry.buildCondaYaml("""
                name: default
                dependencies:
                - python=3.9.15
                - pip
                - cloudpickle=1.0.0
                - pip:
                  - numpy
                  - --extra-index-url https://pypi.ngc.nvidia.com
                  - --extra-index-url  https://pypy.example.com
                  - --extra-index-urlhttps://pypy.invalid-example.com
                  - scipy
                  - pylzy==1.0.0
                  - serialzy>=1.0.0""");
        Assert.assertNotNull(condaYaml);
        Assert.assertTrue(
                "Extra index url is not in final conda yaml",
                condaYaml.contains("--extra-index-url https://pypi.ngc.nvidia.com"));
        Assert.assertTrue(
                "Extra index url is not in final conda yaml",
                condaYaml.contains("--extra-index-url https://pypy.example.com"));
        Assert.assertEquals(
                "Invalid extra index url is in final conda yaml",
                2,
                Pattern.compile("--extra-index-url").matcher(condaYaml).results().count());
    }

    @Test
    public void testPipWithTrustedHosts() {
        String condaYaml = condaPackageRegistry.buildCondaYaml("""
                name: default
                dependencies:
                - python=3.9.15
                - pip
                - cloudpickle=1.0.0
                - pip:
                  - numpy
                  - --trusted-host pypi.ngc.nvidia.com
                  - --trusted-host    example.com:1234
                  - --trusted-hostinvalid-example.com
                  - scipy
                  - pylzy==1.0.0
                  - serialzy>=1.0.0""");
        Assert.assertNotNull(condaYaml);
        Assert.assertTrue(
                "Trusted host is not in final conda yaml",
                condaYaml.contains("--trusted-host pypi.ngc.nvidia.com"));
        Assert.assertTrue(
                "Trusted host is not in final conda yaml",
                condaYaml.contains("--trusted-host example.com:1234"));
        Assert.assertEquals(
                "Invalid trusted host is in final conda yaml",
                2,
                Pattern.compile("--trusted-host").matcher(condaYaml).results().count());
    }
    @Test
    public void testPipWithNoDeps() {
        String condaYaml = condaPackageRegistry.buildCondaYaml("""
                name: default
                dependencies:
                - python=3.9.15
                - pip
                - cloudpickle=1.0.0
                - pip:
                  - --no-deps
                  - numpy
                  - scipy
                  - pylzy==1.0.0
                  - serialzy>=1.0.0""");
        Assert.assertNotNull(condaYaml);
        Assert.assertTrue("--no-deps is not in conda yaml", condaYaml.contains("--no-deps"));
    }

    @Test
    public void testPipWithoutNoDeps() {
        String condaYaml = condaPackageRegistry.buildCondaYaml("""
                name: default
                dependencies:
                - python=3.9.15
                - pip
                - cloudpickle=1.0.0
                - pip:
                  - numpy
                  - scipy
                  - pylzy==1.0.0
                  - serialzy>=1.0.0""");
        Assert.assertNotNull(condaYaml);
        Assert.assertFalse("--no-deps is in conda yaml", condaYaml.contains("--no-deps"));
    }

    @Test
    public void testInvalidYaml() {
        Assert.assertThrows(Exception.class, () -> condaPackageRegistry.buildCondaYaml("///////"));
    }

    @Test
    public void testNotPipDictNotRegistered() {
        condaPackageRegistry.notifyInstalled("""
            name: custom
            dependencies:
            - pip2:
              - pylzy==1.0.0""");
        Assert.assertThrows(IllegalArgumentException.class, () -> condaPackageRegistry.buildCondaYaml("""
            name: custom
            dependencies:
            - pip2:
              - pylzy==1.0.0"""));
    }

    @Test
    public void testInvalidYamlRegister() {
        try {
            condaPackageRegistry.notifyInstalled("///////");
        } catch (Exception e) {
            Assert.fail();
        }
    }
}
