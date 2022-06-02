FROM jupyter/scipy-notebook:latest


COPY jupyter/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
RUN jupyter labextension install @jupyter-widgets/jupyterlab-manager
RUN jupyter labextension install @j123npm/qgrid2@1.1.4