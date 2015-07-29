echo "running 1_medusa_crawler_localidades.py ..."
python 1_medusa_crawler_localidades.py

echo "Running 2_medusa_crawler_prevision.py ..."
python 2_medusa_crawler_prevision.py

echo "Running 3_medusa_crawler_costas.py ..."
python 3_medusa_crawler_costas.py

echo "Running 4_medusa_crawler_cruzroja.py"
scrapy runspider 4_medusa_crawler_cruzroja.py

echo "Running 5_medusa_actualiza_id.py ..."
python 5_medusa_actualiza_id.py

echo "Running 6_medusa_xml_buscador.py ..."
python 6_medusa_xml_buscador.py
