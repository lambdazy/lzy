package ai.lzy.worker.env;


import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

public class CondaPackageRegistryTest {
    @BeforeClass
    public static void before() {
        CondaPackageRegistry.notifyInstalled("""
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
        Assert.assertTrue(CondaPackageRegistry.isInstalled("""
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
        Assert.assertTrue(CondaPackageRegistry.isInstalled("""
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
        Assert.assertTrue(CondaPackageRegistry.isInstalled("""
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
        Assert.assertFalse(CondaPackageRegistry.isInstalled("""
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
        Assert.assertTrue(CondaPackageRegistry.isInstalled("""
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
        Assert.assertTrue(CondaPackageRegistry.isInstalled("""
            name: default
            dependencies:
            - python=3.9.15
            - pip
            - cloudpickle=1.0.0"""));
    }

    @Test
    public void testNoPip() {
        Assert.assertTrue(CondaPackageRegistry.isInstalled("""
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
        Assert.assertFalse(CondaPackageRegistry.isInstalled("""
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
        Assert.assertFalse(CondaPackageRegistry.isInstalled("""
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
        Assert.assertFalse(CondaPackageRegistry.isInstalled("""
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
    public void testTopLevelLibAnotherVersion() {
        Assert.assertFalse(CondaPackageRegistry.isInstalled("""
            name: default
            dependencies:
            - python=3.9.15
            - pip
            - cloudpickle=1.0.1
            - pip:
              - numpy
              - pylzy==1.0.0
              - serialzy>=1.0.0"""));
    }

    @Test
    public void testTopLevelLibInvalid() {
        Assert.assertFalse(CondaPackageRegistry.isInstalled("""
            name: default
            dependencies:
            - python=3.9.15
            - pip
            - 1
            - cloudpickle=1.0.0
            - pip:
              - numpy
              - pylzy==1.0.0
              - serialzy>=1.0.0"""));
    }

    @Test
    public void testTopLevelLibInequality() {
        // currently we do not support inequalities parsing
        Assert.assertFalse(CondaPackageRegistry.isInstalled("""
            name: default
            dependencies:
            - python=3.9.15
            - pip
            - cloudpickle<=1.0.1
            - pip:
              - numpy
              - pylzy==1.0.0
              - serialzy>=1.0.0"""));
    }

    @Test
    public void testSameAnotherName() {
        Assert.assertFalse(CondaPackageRegistry.isInstalled("""
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
    public void testNewTopLevelLibWithoutVersion() {
        Assert.assertFalse(CondaPackageRegistry.isInstalled("""
            name: default
            dependencies:
            - python=3.9.15
            - pip
            - pip2
            - cloudpickle=1.0.0
            - pip:
              - numpy
              - pylzy==1.0.0
              - serialzy>=1.0.0"""));
    }

    @Test
    public void testNewTopLevelLibWithVersion() {
        Assert.assertFalse(CondaPackageRegistry.isInstalled("""
            name: default
            dependencies:
            - python=3.9.15
            - pip
            - pip2<1.0.0
            - cloudpickle=1.0.0
            - pip:
              - numpy
              - pylzy==1.0.0
              - serialzy>=1.0.0"""));
    }

    @Test
    public void testInvalidPipDependenciesName() {
        Assert.assertFalse(CondaPackageRegistry.isInstalled("""
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
        Assert.assertFalse(CondaPackageRegistry.isInstalled("""
            name: default
            dependencies:
            - python=3.9.15
            - pip
            - cloudpickle=1.0.0
            - pip: pep"""));
    }

    @Test
    public void testPipDependenciesIsList() {
        Assert.assertFalse(CondaPackageRegistry.isInstalled("""
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
        Assert.assertFalse(CondaPackageRegistry.isInstalled("""
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
        Assert.assertFalse(CondaPackageRegistry.isInstalled("""
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
    public void testInvalidYaml() {
        Assert.assertFalse(CondaPackageRegistry.isInstalled("///////"));
    }

    @Test
    public void testNotPipDictNotRegistered() {
        CondaPackageRegistry.notifyInstalled("""
            name: custom
            dependencies:
            - pip2:
              - pylzy==1.0.0""");
        Assert.assertFalse(CondaPackageRegistry.isInstalled("""
            name: custom
            dependencies:
            - pip2:
              - pylzy==1.0.0"""));
    }

    @Test
    public void testInvalidYamlRegister() {
        try {
            CondaPackageRegistry.notifyInstalled("///////");
        } catch (Exception e) {
            Assert.fail();
        }
    }
}
