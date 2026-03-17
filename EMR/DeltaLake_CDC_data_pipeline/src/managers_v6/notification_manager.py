import boto3
from datetime import date
import traceback
# ------------------------------------------------------------------------------------------------------------
# print operation name in the log
# ------------------------------------------------------------------------------------------------------------
class SNSPublish:

    def email_notification(self,aws_region,arn,custom_message,env,subject):
        try:
            client = boto3.client('sns', region_name=aws_region)
            response = client.publish(TopicArn = arn,                              
                                       Message = custom_message,
                                       Subject = subject + "::" + env + ":::" + str(date.today())
                                       )
            return response
        except Exception as e :
            print(f'following exception occured in email_notification\n{traceback.format_exc(e)}')
            traceback.format_exc()
            exit(1)
    
