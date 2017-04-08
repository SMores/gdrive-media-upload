import httplib2
import os
import mimetypes
import time

from apiclient import discovery
from googleapiclient.http import MediaFileUpload
from oauth2client import client, tools
from oauth2client.file import Storage

from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler


SCOPES = 'https://www.googleapis.com/auth/drive'
CLIENT_SECRET_FILE = '.google-api-keys.json'
APPLICATION_NAME = 'FlexGet-GDrive'


class Watcher(object):
    """Watchdog wrapper.

    Runs GoogleDriveUpload#on_any_event on any filesystem event in the
    default directory.
    """

    DIRECTORY_TO_WATCH = '/Users/shanewmoore/Media'

    def __init__(self):
        """Simple constructor."""
        self.observer = Observer()

    def run(self):
        """Schedule the observer and start it.

        Will keep this process alive unless an exception is thrown, in which
        case we stop the observer and end the parent process.
        """
        self.observer.schedule(GoogleDriveUpload(), self.DIRECTORY_TO_WATCH, recursive=True)
        print("Starting watchdogs")
        self.observer.start()
        while True:
            try:
                time.sleep(5)
            except Exception as e:
                print(e)
                break

        self.observer.stop()
        self.observer.join()


class GoogleDriveUpload(FileSystemEventHandler):
    """Filesystem event handler.

    Manages process of uploading newly added files to Google Drive.
    """

    def __init__(self):
        """Get the credentials from storage and create a new Drive service instance."""
        credentials = self._get_credentials()
        http = credentials.authorize(httplib2.Http())
        self.service = discovery.build('drive', 'v3', http=http)

    def _get_credentials(self):
        """Retrieve credentials from storage.

        Checks for credentials in the proper home directory location. If
        unavailable, retrieves from the provided json file and persists
        to the proper home directory.
        """
        home_dir = os.path.expanduser('~')
        credential_dir = os.path.join(home_dir, '.credentials')
        if not os.path.exists(credential_dir):
            os.makedirs(credential_dir)
        credential_path = os.path.join(credential_dir, 'drive-python-flexget.json')

        store = Storage(credential_path)
        credentials = store.get()
        if not credentials or credentials.invalid:
            flow = client.flow_from_clientsecrets(CLIENT_SECRET_FILE, SCOPES)
            flow.user_agent = APPLICATION_NAME
            credentials = tools.run_flow(flow, store)
            print('Storing credentials to ' + credential_path)
        return credentials

    def on_any_event(self, event):
        """Called for any watchdog filesystem event.

        Ignores directory changes and .DS_Store changes. Only watches
        new file creation and file renames; in both cases verifying that
        a full file is present (not a .part file from Transmission).
        """
        if event.is_directory:
            return
        if (event.event_type == 'created' and not event.src_path.endswith('.DS_Store') and
                not event.src_path.endswith('.part')):
            try:
                return self.upload_to_google_drive(event.src_path)
            except Exception as e:
                print("Failed to upload new file: {0}\n{1}".format(event.src_path, e))
        if event.event_type == 'moved' and not event.dest_path.endswith('.part'):
            try:
                return self.upload_to_google_drive(event.dest_path)
            except Exception as e:
                print("Failed to upload new file: {0}\n{1}".format(event.dest_path, e))

    def _get_file_id(self, filename):
        """Attempt to retrieve the Google Drive ID of the provided filename.

        Returns None if file is unavailable.
        """
        try:
            return self.service.files().list(
                q="name = '{}'".format(filename)
            ).execute()['files'][0]['id']
        except IndexError:
            return None

    def _find_unsynced_directories(self, dir_list):
        """Determine which of the provided directories already exist in Google Drive.

        Returns a tuple of the name of the last directory along the path
        that *is* available in Drive and the list of directories that need
        to be created.
        """
        parents = [self._get_file_id(parentfolder) for parentfolder in dir_list]
        try:
            first_unsynced = parents.index(None)
            return dir_list[first_unsynced - 1], dir_list[first_unsynced:]
        except ValueError:
            return dir_list[-1], []

    def _fill_in_parents(self, start, rest):
        """Create the directories along the path that are not yet in Drive."""
        parent_id = self._get_file_id(start)
        for dirname in rest:
            metadata = {
                'name': dirname,
                'mimeType': 'application/vnd.google-apps.folder',
                'parents': [parent_id]
            }
            parent_id = self._create_file(body=metadata)
        return parent_id

    def _create_file(self, body={}, media_body=None):
        if media_body is None:
            folder = self.service.files().create(body=body, fields='id').execute()
            return folder.get('id')
        request = self.service.files().create(body=body, media_body=media_body)
        return self._upload_file(request, body['name'])

    def _upload_file(self, request, name, retries=10):
        """Attempt to execute a resumable media upload request.

        Uses TQDM to output a progress bar. Uploads the file in chunks and
        tries to resume on failure. Defaults to 10 initial retries.
        Should implement an exponential back off but currently does not.
        """
        response = None
        print('Uploading {}'.format(name))
        with tqdm(total=1) as pbar:
            while response is None:
                try:
                    status, response = request.next_chunk()
                except Exception as e:
                    print(e)
                    print("Resuming...")
                    return self._upload_file(request, name, retries=retries - 1)
                if status:
                    pbar.update(status.progress() - pbar.n)
        print('Completed upload of {0}'.format(name))
        return response

    def upload_to_google_drive(self, new_file):
        """Upload a file to Google Drive.

        Determines the necessary metadata for an upload, creates the necessary
        parent directories, and uploads the file to the proper directory.
        Directory structure in Drive mimics the input: this function (in fact
        this whole script) expects the 'Media' directory to be in the filepath,
        and will use this directory as the Google Drive root.
        """
        print("Uploading new file to drive: {}".format(new_file))

        mimetype_guess = mimetypes.guess_type(new_file)[0]
        media = MediaFileUpload(new_file, mimetype=mimetype_guess, resumable=True)
        dir_list = new_file.split('/')
        media_index = dir_list.index('Media')
        dir_list = dir_list[media_index + 1:-1]
        last_synced, unsynced_parents = self._find_unsynced_directories(dir_list)
        print("Need to create parent folders '{}'".format("', '".join(unsynced_parents)))
        last_parent_id = self._fill_in_parents(last_synced, unsynced_parents)
        print("Successfully created parent folders. Uploading file...")
        metadata = {
            'name': new_file.split('/')[-1],
            'mimeType': mimetype_guess,
            'parents': [last_parent_id]
        }
        self._create_file(body=metadata, media_body=media)


if __name__ == '__main__':
    w = Watcher()
    w.run()
