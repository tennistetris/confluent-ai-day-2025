# OCR Consumer for Kafka

This directory contains scripts for publishing images to Kafka and consuming them for OCR processing.

## Files

- `quick_publish.py` - Publishes images to the `raw-data` Kafka topic
- `ocr_consumer.py` - Consumes images, calls OCR endpoint, and publishes parsed text
- `text_consumer.py` - Consumes parsed text from the `parsed-ocr-data` topic  
- `test_ocr_consumer.py` - Test script with mock OCR endpoint
- `sagemaker-dpl/deploy.py` - Contains code for sagemaker deployment

## Setup

1. **Activate your virtual environment:**
   ```bash
   source ../venv/bin/activate
   ```

2. **Install dependencies:**
   ```bash
   pip install confluent-kafka requests flask
   ```

## Usage

### 1. Testing with Mock OCR Endpoint

First, test the consumer with a mock endpoint:

```bash
# Terminal 1: Start the test consumer (includes mock server)
python test_ocr_consumer.py

# Terminal 2: Publish an image
python quick_publish.py my_image.png
```

The test consumer will:
- Start a mock OCR server on localhost:5000
- Connect to Kafka and wait for messages
- Process images and return mock markdown

### 2. Using with Your SageMaker Endpoint

Replace the mock endpoint with your actual SageMaker endpoint:

```bash
# Method 1: Command line argument
python ocr_consumer.py https://your-sagemaker-endpoint.amazonaws.com/invocations

# Method 2: Environment variable
export OCR_ENDPOINT="https://your-sagemaker-endpoint.amazonaws.com/invocations"
export OCR_API_KEY="your-api-key-if-needed"
python ocr_consumer.py
```

### 3. Publishing Images

Publish any image to the Kafka topic:

```bash
python quick_publish.py path/to/your/image.jpg
```

## How It Works

1. **Publisher** (`quick_publish.py`):
   - Reads image file as binary data
   - Publishes to `raw-data` topic with filename as key

2. **OCR Consumer** (`ocr_consumer.py`):
   - Subscribes to `raw-data` topic
   - Receives image data and filename
   - Converts image to base64
   - Calls OCR endpoint with payload:
     ```json
     {
       "image": "base64_encoded_image",
       "filename": "original_filename.jpg",
       "format": "markdown"
     }
     ```
   - Extracts raw text from markdown response
   - Publishes parsed text to `parsed-ocr-data` topic
   - Saves both markdown and raw text to files

3. **Text Consumer** (`text_consumer.py`):
   - Subscribes to `parsed-ocr-data` topic
   - Receives parsed text with metadata
   - Displays and saves the clean text output

## OCR Endpoint Format

Your SageMaker endpoint should accept:
- **Method**: POST
- **Headers**: `Content-Type: application/json`
- **Payload**: 
  ```json
  {
    "image": "base64_encoded_image_data",
    "filename": "image.jpg",
    "format": "markdown"
  }
  ```
- **Response**:
  ```json
  {
    "markdown": "# OCR Result\n\nExtracted text...",
    "status": "success"
  }
  ```

### 4. Viewing Parsed Text

Monitor the parsed text output:

```bash
# View parsed text as it's processed
python text_consumer.py
```

## Configuration

The system uses these Kafka topics:
- **`raw-data`**: Raw image data (input)
- **`parsed-ocr-data`**: Parsed text output (output)

Consumer groups:
- **`ocr-consumer-group`**: For OCR processing
- **`text-consumer-group`**: For text monitoring
- **Auto Offset Reset**: `earliest`

## Output Files

- **Markdown**: `ocr_output_[filename].md`
- **Raw Text**: `ocr_text_[filename].txt`  
- **Parsed Text**: `parsed_text_[filename].txt`

## Troubleshooting

1. **Consumer not receiving messages**: Check that you're publishing to the correct topic
2. **OCR endpoint errors**: Verify the endpoint URL and payload format
3. **Authentication issues**: Set the `OCR_API_KEY` environment variable if needed

## Example Output

```
🚀 Starting OCR consumer...
🔗 OCR Endpoint: https://your-endpoint.amazonaws.com/invocations
⏳ Waiting for messages...
📷 Processing image: my_image.png (81865 bytes)
✅ OCR completed for my_image.png
📝 Markdown output:
--------------------------------------------------
# OCR Result for my_image.png

## Extracted Text
Your OCR results here...
--------------------------------------------------
💾 Saved markdown to: ocr_output_my_image_png.md 