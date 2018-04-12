

import codecs

import os


def write_txt(filepath, text, encoding='utf-8'):
    file = codecs.open(filepath, 'w', encoding=encoding)
    file.write(text)
    file.close()


def read_txt(filepath, encoding='utf-8'):
    file = codecs.open(filepath, 'r', encoding=encoding)
    text = ''
    for line in file:
        text += line.strip() + '\n'
    file.close()
    return text


def write_dataset(filepath, dataset, col_split='\t', encoding='utf-8'):
    file = codecs.open(filepath, 'w', encoding=encoding)
    for data in dataset:
        file.write(col_split.join(data) + '\n')
    file.close()


def read_dataset(filepath, col_split='\t', encoding='utf-8'):
    dataset = list()
    file = codecs.open(filepath, 'r', encoding=encoding)
    for line in file:
        dataset.append(line.strip().split(col_split))
    return dataset
