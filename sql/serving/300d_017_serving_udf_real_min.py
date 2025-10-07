import cachetools, mlflow, pandas

@cachetools.cached(cache={})
def get_model(model_key, model_version):
    return mlflow.pyfunc.load_model(f"models:/{model_key}/{model_version}")

def serve_t(model_key, query):
    model_spec = _get_active_model(model_key)
    model = get_model(model_key, model_spec["version"])
    df = pandas.DataFrame([query])
    res = model.predict(df)
    return res[0]
