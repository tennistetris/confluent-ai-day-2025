# aws_deploy_custom.py
import json
import sagemaker
import boto3
from sagemaker.huggingface import HuggingFaceModel
import tarfile
import os

try:
    role = sagemaker.get_execution_role()
except ValueError:
    iam = boto3.client("iam")
    role = iam.get_role(RoleName="sagemaker_execution_role")["Role"]["Arn"]

# Create custom inference script
inference_script = '''
import json
import torch
from transformers import AutoTokenizer, AutoProcessor, AutoModelForImageTextToText
from PIL import Image
import base64
import io

def model_fn(model_dir):
    """Load the model"""
    print("Loading model...")
    model = AutoModelForImageTextToText.from_pretrained(
        model_dir,
        torch_dtype=torch.bfloat16,
        device_map="auto",
        trust_remote_code=True
    )
    
    tokenizer = AutoTokenizer.from_pretrained(model_dir)
    processor = AutoProcessor.from_pretrained(model_dir)
    
    return {
        "model": model,
        "tokenizer": tokenizer, 
        "processor": processor
    }

def input_fn(request_body, request_content_type):
    """Parse input data"""
    if request_content_type == "application/json":
        input_data = json.loads(request_body)
        return input_data
    else:
        raise ValueError(f"Unsupported content type: {request_content_type}")

def predict_fn(input_data, model_dict):
    """Run inference"""
    model = model_dict["model"]
    processor = model_dict["processor"]
    
    # Get image and prompt from input
    image_b64 = input_data.get("image", "")
    custom_prompt = input_data.get("prompt", "Extract the text from the above document as if you were reading it naturally. Return the tables in html format.")
    
    # Decode base64 image
    if image_b64:
        image_data = base64.b64decode(image_b64)
        image = Image.open(io.BytesIO(image_data))
    else:
        raise ValueError("No image provided")
    
    # Prepare messages
    messages = [
        {"role": "system", "content": "You are a helpful assistant."},
        {"role": "user", "content": [
            {"type": "image", "image": image},
            {"type": "text", "text": custom_prompt},
        ]},
    ]
    
    # Process input
    text = processor.apply_chat_template(messages, tokenize=False, add_generation_prompt=True)
    inputs = processor(text=[text], images=[image], padding=True, return_tensors="pt")
    inputs = inputs.to(model.device)
    
    # Generate
    with torch.no_grad():
        output_ids = model.generate(
            **inputs, 
            max_new_tokens=4096, 
            do_sample=False
        )
    
    # Decode output
    generated_ids = [output_ids[len(input_ids):] for input_ids, output_ids in zip(inputs.input_ids, output_ids)]
    output_text = processor.batch_decode(generated_ids, skip_special_tokens=True, clean_up_tokenization_spaces=True)
    
    return {"generated_text": output_text[0]}

def output_fn(prediction, content_type):
    """Format output"""
    if content_type == "application/json":
        return json.dumps(prediction)
    else:
        raise ValueError(f"Unsupported content type: {content_type}")
'''

# Create requirements.txt
requirements = """
torch>=2.0.0
transformers>=4.37.0
accelerate>=0.20.0
Pillow>=9.0.0
sentencepiece>=0.1.99
qwen-vl-utils
"""

# Save files
os.makedirs("code", exist_ok=True)
with open("code/inference.py", "w") as f:
    f.write(inference_script)
with open("code/requirements.txt", "w") as f:
    f.write(requirements)

# Create model.tar.gz
with tarfile.open("model.tar.gz", "w:gz") as tar:
    tar.add("code", arcname="code")

print("Created model.tar.gz with custom inference code")

# Upload to S3
session = sagemaker.Session()
model_uri = session.upload_data("model.tar.gz", key_prefix="nanonets-ocr")
print(f"Model uploaded to: {model_uri}")

# Create HuggingFace Model
huggingface_model = HuggingFaceModel(
    model_data=model_uri,
    role=role,
    transformers_version="4.37.0",
    pytorch_version="2.1.0",
    py_version="py310",
    env={"HF_MODEL_ID": "nanonets/Nanonets-OCR-s", "HF_TASK": "image-text-to-text"},
)

# Deploy
print("Starting deployment...")
predictor = huggingface_model.deploy(
    initial_instance_count=1,
    instance_type="ml.g5.2xlarge",
    container_startup_health_check_timeout=900,  # 15 minutes
)

print(f"✅ Endpoint deployed successfully!")
print(f"Endpoint name: {predictor.endpoint_name}")
