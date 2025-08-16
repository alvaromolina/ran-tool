from pydrive2.auth import GoogleAuth
from pydrive2.drive import GoogleDrive
import os
from dotenv import load_dotenv


def sync_drive_to_pc(credentials_path, sync_config):
    load_dotenv()
    ROOT_DIRECTORY = os.getenv("ROOT_DIRECTORY")

    # Authentication
    gauth = GoogleAuth()
    gauth.LoadClientConfigFile(credentials_path)
    gauth.LocalWebserverAuth()
    drive = GoogleDrive(gauth)

    allowed_exts = (".zip", ".tar", ".csv", ".xlsx", ".rar")

    for config in sync_config:
        pc_path = os.path.join(ROOT_DIRECTORY, config["pc_path"])
        root_folder_id = config["root_folder_id"]
        subfolder_name = config.get("subfolder_name")  # Optional
        mode = config.get("mode", "delta")
        skip_file_list = config.get("skip_file_list", [])

        if mode not in ["delta", "replace"]:
            raise ValueError(f"Unsupported sync mode: {mode}")

        os.makedirs(pc_path, exist_ok=True)

        # Determine final folder to sync from
        if subfolder_name:
            print(f"\n🔍 Looking for subfolder: {subfolder_name}")
            subfolders = drive.ListFile({
                'q': f"'{root_folder_id}' in parents and mimeType='application/vnd.google-apps.folder' and trashed=false"
            }).GetList()

            subfolder = next((f for f in subfolders if f['title'] == subfolder_name), None)
            if not subfolder:
                print(f"❌ Subfolder not found: {subfolder_name}")
                continue
        else:
            subfolder = {'id': root_folder_id}
            print(f"\n🔍 Using root folder directly: {root_folder_id}")

        print(f"📁 Syncing: {pc_path} ← {subfolder_name or '[root folder]'} [mode: {mode}]")

        # Delete last alphabetical file if delta mode
        if mode == "delta":
            local_files = sorted([
                f for f in os.listdir(pc_path)
                if os.path.isfile(os.path.join(pc_path, f))
            ])
            if local_files:
                file_to_delete = local_files[-1]
                os.remove(os.path.join(pc_path, file_to_delete))
                print(f"🧹 Deleting last alphabetical file: {file_to_delete}")

        # List and process files in the folder
        file_list = drive.ListFile({
            'q': f"'{subfolder['id']}' in parents and trashed=false"
        }).GetList()

        for f in file_list:
            file_name = f['title']

            if file_name in skip_file_list:
                print(f"⏩ Skipped (explicitly excluded): {file_name}")
                continue

            if not file_name.lower().endswith(allowed_exts):
                print(f"⏩ Skipped (unsupported file type in {mode} mode): {file_name}")
                continue

            if mode == "delta" and file_name in os.listdir(pc_path):
                continue

            file_path = os.path.join(pc_path, file_name)
            print(f"⬇ Downloading: {file_name}")
            f.GetContentFile(file_path)

        print(f"✅ Done: {pc_path}")
