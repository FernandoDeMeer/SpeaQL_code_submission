### Debugging the Streamlit Application

To debug the Streamlit dashboard in VS Code, add this configuration to your `.vscode/launch.json`:

```json
{
    "name": "debug streamlit",
    "type": "debugpy",
    "request": "launch",
    "program": "${workspaceFolder}/.venv/bin/streamlit",
    "python": "${workspaceFolder}/.venv/bin/python",
    "console": "integratedTerminal",
    "args": [
        "run",
        "streamlit/sca_streamlit_app.py"
    ]
}
```

This allows you to set breakpoints and debug the dashboard with live user inputs.
