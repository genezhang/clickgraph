#!/bin/bash
# Install graph-notebook extensions in Jupyter container
set -e

echo "Installing graph-notebook..."
pip install --quiet graph-notebook

echo "Installing Jupyter extensions..."
jupyter nbextension install --py --sys-prefix graph_notebook.widgets 2>/dev/null || true
jupyter nbextension enable --py --sys-prefix graph_notebook.widgets 2>/dev/null || true

echo "✓ Graph-notebook extensions installed"
