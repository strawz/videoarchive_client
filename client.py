# -*- coding: utf-8 -*-
import sys
import os
import time
import logging
import hashlib
import requests
import mimetypes
import shutil
from watchdog.observers import Observer
from watchdog.events import LoggingEventHandler
from pydrive.auth import GoogleAuth
from pydrive.drive import GoogleDrive
import settings as s
import api_settings as api_s


def md5sum(afile, blocksize=65536):
    """Посчитать MD5 хэш-сумму файла.

    :param afile: file-like объект с методом .read()
    :returns: строка с MD5 хэш-суммой
    """
    buf = afile.read(blocksize)
    hasher = hashlib.md5()
    while len(buf) > 0:
        hasher.update(buf)
        buf = afile.read(blocksize)
    return hasher.hexdigest()


def check_md5_in_db(md5):
    """Проверить MD5 хеш-сумму файла в базе.

    :param md5: строка с MD5 хеш-суммой, которую нужно проверить
    :returns: True если такой файл есть, False -- если нет
    """
    r = requests.get(api_s.MD5_URL, params={'md5': md5})
    return bool(r.json()['status'])


def add_file_to_db(file_path, md5):
    """Добавить запись о файле в базу.

    :param file_path: путь к файлу
    :param md5: строка с MD5 хеш-суммой файла
    :returns: уникальный id добавленного файла
    """
    payload = {'md5Checksum': md5,
               'filePath': file_path}
    r = requests.post(api_s.FILE_URL, auth=(api_s.USER, api_s.PASS), data=payload)
    return r.json()['id']


def upload_file_to_gdrive(gdrive, file_path):
    """Загрузить файл на Google Drive.

    :param gdrive: авторизованный экземпляр класса GoogleDrive
    :param file_path: путь к файлу
    :returns: id файла на Google Drive
    """
    # Получаем id директории, в которую загружаем файл
    q = ''.join(["'root' in parents ",
                 "and trashed=false"
                 "and mimeType='application/vnd.google-apps.folder'",
                 "and title='ICTArchive'"])
    archive_folder_id = gdrive.ListFile({'q': q}).GetList()[0]['id']

    gfile = gdrive.CreateFile({"title": os.path.basename(file_path),
                              "parents": [{"id": archive_folder_id}]})
    gfile.SetContentFile(file_path)
    gfile.Upload()
    return gfile['id']


def add_metadata_to_file(gdrive, file_id, file_gdrive_id):
    """Добавить метаданные файла из Google Drive в базу данных и
    изначальный путь к файлу в папке s.INBOX_DIR -- в дополнительное
    поле метаданных Google Drive.
    Подразумевается, что файл был недавно добавлен в базу и у него
    определены только два поля: filePath и md5Checksum.

    :param gdrive: авторизованный экземпляр класса pydrive.drive.GoogleDrive
    :param file_id: id файла в базе данных
    :type file_id: int
    :param file_gdrive_id: id файла в Google Drive
    :type file_gdrive_id: str or unicode
    """
    # Получаем текущие значения полей в базе:
    file_db_entry = requests.get(api_s.FILEDETAIL_URL % file_id).json()

    ## Добавляем метаданные из Google Drive в базу данных.
    # Инициализируем объект GoogleDriveFile с нужным id:
    gfile = gdrive.CreateFile({'id': file_gdrive_id})
    # Получаем метаданные, формируем PUT-запрос:
    payload  = {
        'googleDriveFileId': gfile['id'],
        'fileSize': gfile['fileSize'],
        'webContentLink': gfile['webContentLink'],
        'mimeType': gfile['mimeType'],
        # Метаданные, специфичные для видео лучше получать после того, как
        # Google Drive их сгенерирует
        # 'videoWidth': gfile['videoMediaMetadata']['width'],
        # 'videoHeight': gfile['videoMediaMetadata']['height'],
        # 'videoDuration': gfile['videoMediaMetadata']['durationMillis']
    }
    # Добавляем текущие значения полей в базе в формируемый запрос:
    payload['id'] = file_db_entry['id']
    payload['filePath'] = file_db_entry['filePath']
    payload['md5Checksum'] = file_db_entry['md5Checksum']
    # Осуществляем запрос:
    r = requests.put(api_s.FILEDETAIL_URL % file_id, auth=(api_s.USER, api_s.PASS), data=payload)

    ## Добавляем в Google Drive изначальный путь к файлу в папке s.INBOX_DIR.
    # Создаем новое дополнительное поле для файла в Google Drive:
    gdrive.auth.service.properties().insert(
        fileId=file_gdrive_id,
        body={
            'key': 'filePath',
            'value': file_db_entry['filePath'],
            'visibility': 'PUBLIC'
        }
    ).execute()


class ArchiveEventHandler(LoggingEventHandler):
    """Обработчик событий файловой системы.
    При создании новых видеофайлов в директории проверяет, есть ли их
    MD5 хеш-сумма в базе данных. Если такой хеш-суммы в базе нет, то
    добавляет новую запись в базу, файл получает уникальное имя и
    перемещается в папку s.ARCHIVE_DIR. Если такая хеш-сумма уже есть,
    то файл перемещается в папку s.CLONE_DIR.
    Если новый файл -- не видео, то он перемещается в папку s.BROKEN_DIR.
    Записывает все события в лог.
    """

    def __init__(self, gdrive):
        """:param gdrive: авторизованный экземпляр класса pydrive.drive.GoogleDrive"""
        super(ArchiveEventHandler, self).__init__()
        self.gdrive = gdrive

    def on_created(self, event):
        super(ArchiveEventHandler, self).on_created(event)

        if not event.is_directory:
            file_path = event.src_path
            file_md5 = md5sum(open(file_path, 'rb'))
            file_in_db = check_md5_in_db(file_md5)
            file_mime = mimetypes.guess_type(file_path)[0]
            if file_mime:
                file_is_a_video = 'video' in file_mime
            else:
                file_is_a_video = False

            logging.info("New file: %s, MD5: %s, is a video: %s, is in DB: %s" % \
                 (file_path, file_md5, file_is_a_video, file_in_db))

            if file_is_a_video and not file_in_db:
                file_id = add_file_to_db(file_path, file_md5)
                file_ext = os.path.splitext(file_path)[1]
                file_archived_name = ''.join((str(file_id), file_ext))
                file_archived_path = os.path.join(s.ARCHIVE_DIR, file_archived_name)                
                shutil.move(file_path, file_archived_path)
                logging.info("Added file to database: %s, MD5: %s,"
                             " is a video: %s, id: %d" % \
                             (file_path, file_md5, file_is_a_video, file_id))
                file_gdrive_id = upload_file_to_gdrive(self.gdrive, file_archived_path)
                logging.info("Uploaded file to Google Drive: %s, MD5: %s,"
                             " is a video: %s, gdrive_id: %s" % \
                             (file_archived_path, file_md5, file_is_a_video, file_gdrive_id))
                add_metadata_to_file(self.gdrive, file_id, file_gdrive_id)

            elif file_is_a_video and file_in_db:
                shutil.move(file_path, s.CLONE_DIR)
                logging.info("Moved file to duplicates directory: %s" % \
                    os.path.join(s.CLONE_DIR, os.path.basename(file_path)))
            elif not file_is_a_video:
                shutil.move(file_path, s.BROKEN_DIR)
                logging.info("Moved file to non-video directory: %s" % \
                    os.path.join(s.BROKEN_DIR, os.path.basename(file_path)))


if __name__ == "__main__":
    # Авторизация в Google Drive
    gauth = GoogleAuth()
    gauth.LocalWebserverAuth()
    drive = GoogleDrive(gauth)

    logging.basicConfig(level=logging.INFO,
                        format='%(asctime)s - %(message)s',
                        datefmt='%Y-%m-%d %H:%M:%S')
    path = sys.argv[1] if len(sys.argv) > 1 else s.INBOX_DIR
    event_handler = ArchiveEventHandler(drive)
    observer = Observer()
    observer.schedule(event_handler, path, recursive=True)
    observer.start()
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        observer.stop()
    observer.join()
