import re

def transform_fatty_acid_notation(notation_str):
    """
    Преобразует обозначение жирной кислоты из формата 'C:D-X,Y'
    в формат 'fatty_acid!(C<C> { X => DC, Y => DC })?,'.
    """
    notation_str = notation_str.strip()
    
    # Разделяем на основную часть (например, '18:2') и часть с позициями (например, '9,12')
    parts = notation_str.split('-', 1)
    main_part = parts[0]
    positions_part = parts[1] if len(parts) > 1 else None
    
    # Извлекаем количество атомов углерода
    carbon_count = main_part.split(':')[0]
    
    # Форматируем позиции
    if positions_part:
        positions = positions_part.split(',')
        formatted_positions = ", ".join(f"{pos} => DC" for pos in positions)
        return f"fatty_acid!(C{carbon_count} {{ {formatted_positions} }})?,"
    else:
        return f"fatty_acid!(C{carbon_count} {{ }})?,"

def process_markdown_tables(markdown_text):
    """
    Обрабатывает текст в формате Markdown для преобразования первого столбца всех таблиц.
    """
    output_lines = []
    for line in markdown_text.strip().split('\n'):
        # Проверяем, является ли строка строкой данных таблицы
        # (начинается с '|' и не является строкой заголовка или разделителя)
        is_data_row = line.strip().startswith('|') and '---' not in line and 'Обозначение' not in line

        if is_data_row:
            cells = line.split('|')
            # cells[0] - пустая строка до первого '|'
            # cells[1] - содержимое первого столбца
            if len(cells) > 2:  # Убеждаемся, что это действительная строка таблицы с содержимым
                original_notation = cells[1].strip()
                if original_notation: # Проверяем, что ячейка не пустая
                    transformed_notation = transform_fatty_acid_notation(original_notation)
                    # Заменяем содержимое первой ячейки, сохраняя отступы
                    cells[1] = f" {transformed_notation} "
                    output_lines.append("|".join(cells))
                else:
                    output_lines.append(line) # Оставляем пустые строки как есть
            else:
                output_lines.append(line) # Не строка, которую мы хотим обработать
        
        # Корректируем строку-разделитель, чтобы она соответствовала новому, более длинному содержимому
        elif "| :---" in line:
            parts = line.split('|')
            # Делаем разделитель первого столбца значительно длиннее
            parts[1] = ' :------------------------------------------------- '
            output_lines.append("|".join(parts))
        else:
            # Сохраняем строки, не являющиеся табличными, как есть
            output_lines.append(line)
            
    return "\n".join(output_lines)

input_file = '_data/2025.Acers/INPUT.md'
output_file = '_data/2025.Acers/OUTPUT.md'

# Чтение из файла input.md
with open(input_file, 'r', encoding='utf-8') as f:
    markdown_input = f.read()

# Обрабатываем Markdown и выводим результат
output_markdown = process_markdown_tables(markdown_input)

# Запись в файл output.md
with open(output_file, 'w', encoding='utf-8') as f:
    f.write(output_markdown)

print(f"Преобразован файл '{input_file}'")
