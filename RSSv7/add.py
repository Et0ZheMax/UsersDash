import os
import stat

# Указываем путь к папке
folder_path = r'C:\LDPlayer\LDPlayer9\vms\config'

# Проверяем, существует ли папка
if os.path.exists(folder_path):
    for root, dirs, files in os.walk(folder_path):
        for name in dirs + files:
            file_path = os.path.join(root, name)
            try:
                # Получаем текущие атрибуты файла/папки
                file_stat = os.stat(file_path)
                
                # Убираем атрибут "только для чтения"
                os.chmod(file_path, file_stat.st_mode | stat.S_IWRITE)
                
                print(f"Галочка 'Только чтение' снята с: {file_path}")
            except Exception as e:
                print(f"Ошибка при изменении {file_path}: {e}")
else:
    print(f"Папка не найдена: {folder_path}")
