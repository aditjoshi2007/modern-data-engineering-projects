import boto3
import redis
import base64
from botocore.exceptions import ClientError

def get_secret(secret_name):

    region_name = "aws_region"

    # Create a Secrets Manager client
    session = boto3.session.Session()
    client = session.client(
        service_name='secretsmanager',
        region_name=region_name
    )

    # In this sample we only handle the specific exceptions for the 'GetSecretValue' API.
    # See https://docs.aws.amazon.com/secretsmanager/latest/apireference/API_GetSecretValue.html
    # We rethrow the exception by default.

    try:
        get_secret_value_response = client.get_secret_value(
            SecretId=secret_name
        )
    except ClientError as e:
        if e.response['Error']['Code'] == 'DecryptionFailureException':
            # Secrets Manager can't decrypt the protected secret text using the provided KMS key.
            # Deal with the exception here, and/or rethrow at your discretion.
            raise e
        elif e.response['Error']['Code'] == 'InternalServiceErrorException':
            # An error occurred on the server side.
            # Deal with the exception here, and/or rethrow at your discretion.
            raise e
        elif e.response['Error']['Code'] == 'InvalidParameterException':
            # You provided an invalid value for a parameter.
            # Deal with the exception here, and/or rethrow at your discretion.
            raise e
        elif e.response['Error']['Code'] == 'InvalidRequestException':
            # You provided a parameter value that is not valid for the current state of the resource.
            # Deal with the exception here, and/or rethrow at your discretion.
            raise e
        elif e.response['Error']['Code'] == 'ResourceNotFoundException':
            # We can't find the resource that you asked for.
            # Deal with the exception here, and/or rethrow at your discretion.
            raise e
    else:
        # Decrypts secret using the associated KMS CMK.
        # Depending on whether the secret is a string or binary, one of these fields will be populated.
        if 'SecretString' in get_secret_value_response:
            secret = get_secret_value_response['SecretString']
            return (secret)
        else:
            decoded_binary_secret = base64.b64decode(get_secret_value_response['SecretBinary'])
            return (decoded_binary_secret)
        
def get_ssm_parameter(parameter_name):
	#*** Get parameter from the parameter store
	parameter = ssm_client.get_parameter(Name=parameter_name)
	return parameter['Parameter']['Value']

def get_redis_parameter(parameter_name):
	# Get Redis parameter value
	return redis_client.get(parameter_name)

def set_redis_parameter(parameter_name, value):
	# Set Redis parameter value
	return redis_client.set(parameter_name, value)

def get_redis_client():
    return redis_client

def get_redis_hash_parameter(parameter_name, parameter_key):
    return  redis_client.hget(parameter_name, parameter_key)
    
def set_redis_hash_parameter(parameter_name, parameter_key, parameter_value):
    return redis_client.hset(parameter_name, parameter_key, parameter_value)


def get_all_keys():
    return redis_client.keys('*')

#***********************************************************************************************
# *** Initialize ***
#***********************************************************************************************
ssm_client = boto3.client('ssm', "aws_region")

#*** Get Redis object ***
redis_client = redis.StrictRedis(host=get_ssm_parameter('REDIS_INSTANCE'), port=1234, db=0, decode_responses=True, ssl=True)


