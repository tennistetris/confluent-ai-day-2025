from confluent_kafka import Consumer, KafkaError, KafkaException
import json
import sys

class TextConsumer:
    def __init__(self):
        """
        Initialize consumer for parsed-ocr-data topic
        """
        self.consumer_config = {
            'bootstrap.servers': 'pkc-921jm.us-east-2.aws.confluent.cloud:9092',
            'security.protocol': 'SASL_SSL',
            'sasl.mechanisms': 'PLAIN',
            'sasl.username': 'THV3Q2775Y3Z3I5M',
            'sasl.password': 'LPUb0KrISli6HdCgUhCZp22LTApB+UtGWp7ihLTbULGbqerG9DKN/61ZaLIAJqiM',
            'group.id': 'text-consumer-group',
            'auto.offset.reset': 'earliest'
        }
        
        self.consumer = Consumer(self.consumer_config)
    
    def process_text_message(self, message):
        """
        Process a parsed text message
        """
        try:
            # Get filename from key
            filename = message.key().decode('utf-8') if message.key() else "unknown"
            
            # Parse the JSON payload
            data = json.loads(message.value().decode('utf-8'))
            
            raw_text = data.get('raw_text', '')
            text_length = data.get('text_length', len(raw_text))
            processed_at = data.get('processed_at', 'unknown')
            
            print(f"\n📄 Received parsed text for: {filename}")
            print(f"📊 Text length: {text_length} characters")
            print(f"⏰ Processed at: {processed_at}")
            print("=" * 60)
            print(raw_text)
            print("=" * 60)
            
            # Save to file
            output_file = f"parsed_text_{filename.replace('.', '_')}.txt"
            with open(output_file, 'w', encoding='utf-8') as f:
                f.write(raw_text)
            print(f"💾 Saved parsed text to: {output_file}")
            
        except Exception as e:
            print(f"❌ Error processing text message: {e}")
    
    def start_consuming(self):
        """
        Start consuming from parsed-ocr-data topic
        """
        try:
            self.consumer.subscribe(['parsed-ocr-data'])
            print("🚀 Starting text consumer...")
            print("📖 Listening for parsed OCR text on 'parsed-ocr-data' topic")
            print("⏳ Waiting for messages...")
            
            while True:
                msg = self.consumer.poll(timeout=1.0)
                
                if msg is None:
                    continue
                
                if msg.error():
                    if msg.error().code() == KafkaError._PARTITION_EOF:
                        continue
                    else:
                        print(f"❌ Consumer error: {msg.error()}")
                        break
                
                self.process_text_message(msg)
                
        except KeyboardInterrupt:
            print("\n🛑 Shutting down text consumer...")
        except KafkaException as e:
            print(f"❌ Kafka error: {e}")
        finally:
            self.consumer.close()
            print("👋 Text consumer closed")

def main():
    """
    Main function to run the text consumer
    """
    consumer = TextConsumer()
    consumer.start_consuming()

if __name__ == "__main__":
    main() 