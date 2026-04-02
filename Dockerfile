FROM public.ecr.aws/docker/library/python:3.12-slim

# Set timezone to GMT+7 (Asia/Novosibirsk)
ENV TZ=Asia/Novosibirsk
RUN ln -snf /usr/share/zoneinfo/$TZ /etc/localtime && echo $TZ > /etc/timezone

WORKDIR /app

# Install dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy bot code
COPY . .

# Create data directory
RUN mkdir -p /app/data

# Run bot
CMD ["python", "main.py"]
