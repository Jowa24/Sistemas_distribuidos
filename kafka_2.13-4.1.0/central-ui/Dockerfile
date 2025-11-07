FROM python:3.9

WORKDIR /app

COPY requirements.txt .
RUN pip install -r requirements.txt

COPY app/ .

# Create templates directory
RUN mkdir -p templates
COPY app/templates/ ./templates/

EXPOSE 5000

CMD ["python", "main.py"]
