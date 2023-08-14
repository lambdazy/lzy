from setuptools import Extension, setup

setup(
    ext_modules=[
        Extension(
            name="lzy_test_project.foo",
            sources=["foo.c"],
        ),
    ]
)
