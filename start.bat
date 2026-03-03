@echo off
echo ========================================
echo  Navedas Governance - Streamlit App
echo  Opening at: http://localhost:8501
echo ========================================
cd /d "%~dp0"
streamlit run app.py --server.port 8501
pause
