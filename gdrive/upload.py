from gdrive import service
from log_helper import get_logger
from googleapiclient.http import MediaFileUpload

logger = get_logger('GoogleDrive Service')

drive_service = service.drive_service()

def upload_file(file_name):
    media = MediaFileUpload(file_name, resumable=True)

    request = drive_service.files().create(fields='id,webViewLink',body={'name':file_name.split('/')[-1],'parents':['168yOSRkAEYXLzoqWeGLzLzPM-8oVv8Xm']},media_body=media)

    response = None
    logger.info(f'Uploading {file_name}')

    while response is None:
        status, response = request.next_chunk()
        if status:
            logger.info("Uploaded %d%%." % int(status.progress() * 100))
    logger.info("Upload Complete!")
    return response['webViewLink'] 
    