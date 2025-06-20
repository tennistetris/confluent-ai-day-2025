from confluent_kafka import Consumer, Producer, KafkaError, KafkaException
import requests
import base64
import json
import sys
import os
import re
from typing import Optional

class OCRConsumer:
    def __init__(self, ocr_endpoint: str, api_key: Optional[str] = None):
        """
        Initialize the OCR Consumer
        
        Args:
            ocr_endpoint: The OCR API endpoint URL
            api_key: Optional API key for authentication
        """
        # Kafka consumer configuration
        self.consumer_config = {
            'bootstrap.servers': 'pkc-921jm.us-east-2.aws.confluent.cloud:9092',
            'security.protocol': 'SASL_SSL',
            'sasl.mechanisms': 'PLAIN',
            'sasl.username': 'THV3Q2775Y3Z3I5M',
            'sasl.password': 'LPUb0KrISli6HdCgUhCZp22LTApB+UtGWp7ihLTbULGbqerG9DKN/61ZaLIAJqiM',
            'group.id': 'ocr-consumer-group',
            'auto.offset.reset': 'earliest'
        }
        
        # Kafka producer configuration (same credentials)
        self.producer_config = {
            'bootstrap.servers': 'pkc-921jm.us-east-2.aws.confluent.cloud:9092',
            'security.protocol': 'SASL_SSL',
            'sasl.mechanisms': 'PLAIN',
            'sasl.username': 'THV3Q2775Y3Z3I5M',
            'sasl.password': 'LPUb0KrISli6HdCgUhCZp22LTApB+UtGWp7ihLTbULGbqerG9DKN/61ZaLIAJqiM',
        }
        
        self.consumer = Consumer(self.consumer_config)
        self.producer = Producer(self.producer_config)
        self.ocr_endpoint = ocr_endpoint
        self.api_key = api_key
        
        # Setup headers for OCR API
        self.headers = {'Content-Type': 'application/json'}
        if api_key:
            self.headers['Authorization'] = f'Bearer {api_key}'
    
    def call_ocr_endpoint(self, image_data: bytes, filename: str) -> dict:
        """
        Call the OCR endpoint with image data
        
        Args:
            image_data: Raw image bytes
            filename: Original filename for reference
            
        Returns:
            OCR response as dictionary
        """
        try:
            # Convert image to base64
            image_base64 = base64.b64encode(image_data).decode('utf-8')
            
            # Prepare payload (adjust based on your actual SageMaker endpoint format)
            payload = {
                "image": image_base64,
                "filename": filename,
                "format": "markdown"  # Request markdown output
            }
            
            # Make API request
            response = requests.post(
                self.ocr_endpoint,
                headers=self.headers,
                json=payload,
                timeout=60  # 60 second timeout for OCR processing
            )
            
            if response.status_code == 200:
                return response.json()
            else:
                return {
                    "error": f"OCR API failed with status {response.status_code}",
                    "details": response.text
                }
                
        except requests.exceptions.Timeout:
            return {"error": "OCR request timed out"}
        except requests.exceptions.RequestException as e:
            return {"error": f"OCR request failed: {str(e)}"}
        except Exception as e:
            return {"error": f"Unexpected error: {str(e)}"}
    
    def extract_raw_text(self, markdown_content: str) -> str:
        """
        Extract raw text from markdown content by removing formatting
        
        Args:
            markdown_content: Markdown formatted text
            
        Returns:
            Plain text without markdown formatting
        """
        try:
            # Remove markdown headers
            text = re.sub(r'^#{1,6}\s+', '', markdown_content, flags=re.MULTILINE)
            
            # Remove bold and italic formatting
            text = re.sub(r'\*\*([^*]+)\*\*', r'\1', text)  # **bold**
            text = re.sub(r'\*([^*]+)\*', r'\1', text)      # *italic*
            text = re.sub(r'__([^_]+)__', r'\1', text)      # __bold__
            text = re.sub(r'_([^_]+)_', r'\1', text)        # _italic_
            
            # Remove links but keep text
            text = re.sub(r'\[([^\]]+)\]\([^)]+\)', r'\1', text)
            
            # Remove code blocks
            text = re.sub(r'```[^`]*```', '', text, flags=re.DOTALL)
            text = re.sub(r'`([^`]+)`', r'\1', text)
            
            # Remove LaTeX equations
            text = re.sub(r'\$\$[^$]*\$\$', '', text, flags=re.DOTALL)
            text = re.sub(r'\$([^$]+)\$', r'\1', text)
            
            # Clean up table formatting - extract content between pipes
            lines = text.split('\n')
            cleaned_lines = []
            for line in lines:
                if '|' in line and not line.strip().startswith('|---'):
                    # Extract table cell content
                    cells = [cell.strip() for cell in line.split('|')[1:-1]]
                    cleaned_lines.append(' '.join(cells))
                elif not line.strip().startswith('|---'):
                    cleaned_lines.append(line)
            
            text = '\n'.join(cleaned_lines)
            
            # Remove horizontal rules
            text = re.sub(r'^-{3,}$', '', text, flags=re.MULTILINE)
            
            # Clean up multiple newlines and whitespace
            text = re.sub(r'\n\s*\n', '\n\n', text)
            text = re.sub(r'[ \t]+', ' ', text)
            
            return text.strip()
            
        except Exception as e:
            print(f"⚠️  Error extracting raw text: {e}")
            return markdown_content  # Return original if extraction fails
    
    def publish_parsed_text(self, raw_text: str, filename: str) -> bool:
        """
        Publish parsed raw text to the parsed-ocr-data topic
        
        Args:
            raw_text: The extracted plain text
            filename: Original filename to use as key
            
        Returns:
            True if successful, False otherwise
        """
        try:
            # Create metadata object with the text
            parsed_data = {
                'filename': filename,
                'raw_text': raw_text,
                'processed_at': json.dumps({"timestamp": __import__('time').time()}),
                'text_length': len(raw_text)
            }
            
            # Publish to parsed-ocr-data topic
            self.producer.produce(
                'parsed-ocr-data', 
                key=filename.encode('utf-8'),
                value=json.dumps(parsed_data, ensure_ascii=False).encode('utf-8')
            )
            
            # Flush to ensure delivery
            self.producer.flush()
            
            print(f"📤 Published parsed text to 'parsed-ocr-data' topic ({len(raw_text)} chars)")
            return True
            
        except Exception as e:
            print(f"❌ Failed to publish parsed text: {e}")
            return False
    
    def process_message(self, message):
        """
        Process a single Kafka message
        
        Args:
            message: Kafka message containing image data
        """
        try:
            # Get filename from message key
            filename = message.key().decode('utf-8') if message.key() else "unknown.png"
            image_data = message.value()
            
            print(f"📷 Processing image: {filename} ({len(image_data)} bytes)")
            
            # Call OCR endpoint
            ocr_result = self.call_ocr_endpoint(image_data, filename)
            
            if "error" in ocr_result:
                print(f"❌ OCR failed for {filename}: {ocr_result['error']}")
                return
            
            # Extract markdown from result (adjust key based on your endpoint response)
            markdown_content = ocr_result.get('markdown', ocr_result.get('text', str(ocr_result)))
            
            print(f"✅ OCR completed for {filename}")
            print(f"📝 Markdown output:")
            print("-" * 50)
            print(markdown_content)
            print("-" * 50)
            
            # Extract raw text from markdown
            raw_text = self.extract_raw_text(markdown_content)
            
            print(f"📄 Raw text extracted:")
            print("-" * 30)
            print(raw_text[:500] + "..." if len(raw_text) > 500 else raw_text)
            print("-" * 30)
            
            # Publish raw text to parsed-ocr-data topic
            success = self.publish_parsed_text(raw_text, filename)
            if not success:
                print(f"⚠️  Failed to publish parsed text for {filename}")
            
            # Optional: Save markdown to file
            output_filename = f"ocr_output_{filename.replace('.', '_')}.md"
            with open(output_filename, 'w', encoding='utf-8') as f:
                f.write(markdown_content)
            print(f"💾 Saved markdown to: {output_filename}")
            
            # Optional: Save raw text to file
            text_filename = f"ocr_text_{filename.replace('.', '_')}.txt"
            with open(text_filename, 'w', encoding='utf-8') as f:
                f.write(raw_text)
            print(f"💾 Saved raw text to: {text_filename}")
            
        except Exception as e:
            print(f"❌ Error processing message: {str(e)}")
    
    def start_consuming(self):
        """
        Start consuming messages from the raw-data topic
        """
        try:
            # Subscribe to the topic
            self.consumer.subscribe(['raw-data'])
            print("🚀 Starting OCR consumer...")
            print(f"🔗 OCR Endpoint: {self.ocr_endpoint}")
            print("⏳ Waiting for messages...")
            
            while True:
                # Poll for messages
                msg = self.consumer.poll(timeout=1.0)
                
                if msg is None:
                    continue
                
                if msg.error():
                    if msg.error().code() == KafkaError._PARTITION_EOF:
                        # End of partition - continue
                        continue
                    else:
                        print(f"❌ Consumer error: {msg.error()}")
                        break
                
                # Process the message
                self.process_message(msg)
                
        except KeyboardInterrupt:
            print("\n🛑 Shutting down consumer...")
        except KafkaException as e:
            print(f"❌ Kafka error: {e}")
        finally:
            # Close the consumer and producer
            self.consumer.close()
            self.producer.flush()  # Ensure all messages are sent
            print("👋 Consumer and producer closed")

def main():
    """
    Main function to run the consumer
    """
    # Default endpoint - replace with your actual SageMaker endpoint
    default_endpoint = "https://your-sagemaker-endpoint.us-east-1.sagemaker.amazonaws.com/invocations"
    
    # Get OCR endpoint from command line or environment
    ocr_endpoint = sys.argv[1] if len(sys.argv) > 1 else os.getenv('OCR_ENDPOINT', default_endpoint)
    api_key = os.getenv('OCR_API_KEY')  # Optional API key from environment
    
    if ocr_endpoint == default_endpoint:
        print("⚠️  Using default endpoint URL. Please provide your actual SageMaker endpoint:")
        print("   python ocr_consumer.py https://your-sagemaker-endpoint.amazonaws.com/invocations")
        print("   OR set OCR_ENDPOINT environment variable")
        return
    
    # Create and start consumer
    consumer = OCRConsumer(ocr_endpoint, api_key)
    consumer.start_consuming()

if __name__ == "__main__":
    main() 