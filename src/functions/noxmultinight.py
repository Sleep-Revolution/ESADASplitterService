from pathlib import Path,PurePath
import os
import json
import time
import datetime
import requests
import zipfile
import requests
import json



def NoxSplitting(file_path_zip, IndNigth_file_repo):

    final_path = os.path.join(IndNigth_file_repo,
                              file_path_zip.split('\\')[-1].split('.')[0])

    try:
        headers = {"accept": "application/json"}
        # If you are sending a real file, you can do something akin to this:
        # files = {"file": open(path_to_my_zip_file, "rb")}
        files = {"file": open(file_path_zip, "rb")}
        t = datetime.datetime.now()
        print(f"Process starting on {datetime.datetime.now()}") 
        r = requests.post(os.environ["NOX_3NSplitting_SERVICE"], headers=headers, files=files, timeout=1000)
        print(f"Process ending after {datetime.datetime.now()-t}")


        try:
            # Save the received zip file content to a temporary file
            temp_zip_file = os.path.join(IndNigth_file_repo, 'temp_received.zip')
            with open(temp_zip_file, 'wb') as f:
                f.write(r.content)

            # Extract the zip file contents to the extraction directory
            with zipfile.ZipFile(temp_zip_file, 'r') as zip_ref:
                list_of_dir = []
                for i in zip_ref.infolist():
                    try:
                        list_of_dir.append(os.path.dirname(i.filename))
                    except:
                        list_of_dir.append('')
                for root, file_path in zip(list_of_dir,zip_ref.infolist()):
                    if root != '':
                        file_path.filename = PurePath(file_path.filename).name
                        zip_ref.extract(file_path,final_path + root.zfill(2))

            print(f"Zip file contents extracted to: {final_path}")

            # Now you can work with the extracted files in the 'extract_dir' directory
            # Do whatever processing you need with the extracted files here
        except Exception as e:
            return False, f"Failed to run NOX splitter {dir}, Error: {str(e)}", ""
        finally:
            # Clean up: Remove the temporary zip file
            os.remove(temp_zip_file)
        return True, "success", final_path
    except Exception as e:
        return False, f"Failed to run NOX splitter {dir}, Error: {str(e)}", ""

# Test the NoxSplitting function
if __name__ == "__main__":
    os.environ["NOX_3NSplitting_SERVICE"] = "http://127.0.0.1:8080/nox-recording-splitter"
    file_path_zip = "//130.208.209.1/Workspace/Benedikt/Pipeline/PortalDestination/EmilSRE/BJEMSLEV0315.zip"
    IndNigth_file_repo ="//130.208.209.1/Workspace/Benedikt/Pipeline/IndividualNightWaitingRoom/EmilSRE/"
    NoxSplitting(file_path_zip, IndNigth_file_repo)