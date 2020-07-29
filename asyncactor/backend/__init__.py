def get_transport(name):
    from importlib import import_module

    if "." not in name:
        name = "asyncactor.backend." + name
    return import_module(name).Transport
