# Use the latest official Python image from the docker hub
FROM python:latest

# Set the working directory inside the container
WORKDIR /app

# Copy the requirements.txt into the container
COPY requirements.txt .

# Install any needed packages specified in requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

# Copy the entire local project
COPY . .

# Expose this port to container
EXPOSE 5000

# Command to run the application
CMD ["python", "main.py"]