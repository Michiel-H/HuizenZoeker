#!/usr/bin/env python3
"""Streamlit Community Cloud entry point.

Streamlit Cloud looks for streamlit_app.py at the repo root by default.
This simply re-exports the dashboard app.
"""

from src.dashboard.app import main

main()
