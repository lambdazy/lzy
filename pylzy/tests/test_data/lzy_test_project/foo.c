#include <Python.h>

static PyObject* foo(PyObject* self)
{
    PyObject* s = PyUnicode_FromString("foo");
    PyObject_Print(s, stdout, 0);
    return s;
}

static PyMethodDef methods[] = {
    {"foo", (PyCFunction)foo, METH_NOARGS, NULL},
    {NULL, NULL, 0, NULL}
};

static struct PyModuleDef module = {
    PyModuleDef_HEAD_INIT, "foo", NULL, -1, methods
};

PyMODINIT_FUNC PyInit_foo(void)
{
    return PyModule_Create(&module);
}
