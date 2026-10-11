FROM python:3.12-slim
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY doctor ./doctor
ENTRYPOINT ["python", "-m", "doctor.server"]
CMD ["--transport", "http", "--host", "0.0.0.0", "--port", "8000"]
