import os
from gdrive import service
from log_helper import get_logger
from googleapiclient.http import MediaFileUpload

logger = get_logger('GoogleDrive Service')

drive_service = service.drive_service()

def upload_file(file_name):
    link_file = f'{file_name}.webViewLink'

    if not os.path.exists(link_file):
        media = MediaFileUpload(file_name, resumable=True)

        request = drive_service.files().create(fields='id,webViewLink',body={'name':file_name.split('/')[-1],'parents':['1twO1fPD1gCC7MgH3WnlMweX8BOtDUMke']},media_body=media)

        response = None
        logger.info(f'Uploading {file_name}')

        while response is None:
            status, response = request.next_chunk()
            if status:
                logger.info("Uploaded %d%%." % int(status.progress() * 100))
        logger.info("Upload Complete!")

        with open(f'{file_name}.webViewLink','w+') as f:
            f.write(response['webViewLink'] )
        
        return response['webViewLink'] 
    with open(link_file) as f:
        return f.read()
    