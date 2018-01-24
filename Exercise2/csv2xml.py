import sys
import pandas as pd
from lxml import etree


if __name__ == '__main__':
    input_file = sys.argv[1]
    output_file = sys.argv[2]

    population_csv = pd.read_csv(input_file, delimiter=';', skiprows=4, header=None)
    population_csv.columns = ['name', 'year', 'total', 'males', 'females']
    
    root = etree.Element('populationdata')
    for index, municipality in population_csv.iterrows():
        municipality_node = etree.Element('municipality')
        for attr, value in municipality.iteritems():
            attr_node = etree.Element(attr)
            attr_node.text = str(value)
            municipality_node.append(attr_node)
        root.append(municipality_node)

    xml_string = etree.tostring(root, pretty_print=True, xml_declaration=True, encoding='utf-8')
    with open(output_file, "wb") as text_file:
        text_file.write(xml_string)
