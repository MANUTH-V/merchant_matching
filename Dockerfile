FROM python:3.7
WORKDIR /app

# Install dependencies
COPY requirements.txt .
RUN pip install -r requirements.txt

# Copy the rest of the code
COPY . .

EXPOSE 8000

# Run the app
CMD ["python", "main.py"]