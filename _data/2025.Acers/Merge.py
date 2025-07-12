import polars as pl
import re
from io import StringIO

def parse_md_file_polars(file_path):
    """
    Парсит markdown файл, извлекая заголовок и таблицы в виде Polars DataFrame.
    """
    with open(file_path, 'r', encoding='utf-8') as f:
        content = f.read()
    
    sections = re.split(r'\n## ', content)
    file_title = sections[0].strip('# ').strip()
    tables = {}
    
    for section in sections[1:]:
        lines = section.strip().split('\n')
        table_title = lines[0].strip()
        
        table_lines = [line for line in lines[1:] if line.strip().startswith('|')]
        
        if not table_lines:
            continue
            
        # Заменяем разделитель '---' на пустую строку, чтобы избежать ошибок парсинга
        table_lines[1] = '|' * (table_lines[0].count('|'))
        table_csv_string = '\n'.join(table_lines)
        
        # Чтение в DataFrame с помощью Polars
        # Используем StringIO для чтения строки как файла
        df = pl.read_csv(StringIO(table_csv_string), separator='|', has_header=True)
        
        # Удаляем пустые столбцы, которые появляются из-за '|' в начале и конце строки
        df = df.select([col for col in df.columns if col.strip() != ""])[1:-1]
        
        # Очищаем имена столбцов и данные
        new_columns = {col: col.strip() for col in df.columns}
        df = df.rename(new_columns)
        
        # Применяем очистку ко всем строковым столбцам и приводим типы
        df = df.with_columns([
            pl.col(pl.Utf8).str.strip_chars(),
            pl.col("Площадь (мВ*с)").cast(pl.Float64, strict=False)
        ])
        
        tables[table_title] = df
        
    return file_title, tables

# Парсинг файлов
file1_name, tables1 = parse_md_file_polars('_data/2025.Acers/_TAG.md')
file2_name, tables2 = parse_md_file_polars('_data/2025.Acers/_MAG.md')

# Получение списка заголовков таблиц для сохранения порядка
titles1 = list(tables1.keys())
titles2 = list(tables2.keys())

# Определение количества пар для объединения
num_pairs = min(len(titles1), len(titles2))

# Попарное объединение и вывод
for i in range(num_pairs):
    title1 = titles1[i]
    title2 = titles2[i]
    df1 = tables1[title1]
    df2 = tables2[title2]
    print(df1)
    # print(f"### Объединенная таблица: '{file1_name} ({title1})' и '{file2_name} ({title2})'\n")
    
    # # Подготовка таблиц к объединению
    # df1_merge = df1.select(['Обозначение', 'Компонент', 'Площадь (мВ*с)'])
    # df2_merge = df2.select(['Обозначение', 'Площадь (мВ*с)'])
    
    # # Переименование столбцов с площадями
    # df1_merge = df1_merge.rename({'Площадь (мВ*с)': f'Площадь (мВ*с) - {file1_name} {title1}'})
    # df2_merge = df2_merge.rename({'Площадь (мВ*с)': f'Площадь (мВ*с) - {file2_name} {title2}'})
    
    # # Внешнее объединение по столбцу 'Обозначение'
    # merged_df = df1_merge.join(df2_merge, on='Обозначение', how='outer')
    
    # # Для вывода в формате markdown с форматированием чисел,
    # # временно конвертируем в pandas, так как у polars нет встроенной опции floatfmt для markdown.
    # print(merged_df.to_pandas().to_markdown(index=False, floatfmt=".3f"))
    # print("\n" + "="*80 + "\n")

# file1_name, tables1 = parse_md_file('_data/2025.Acers/_TAG.md')
# file2_name, tables2 = parse_md_file('_data/2025.Acers/_MAG.md')