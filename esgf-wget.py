import requests
import os
import json
import datetime
import argparse
import jinja2

def get_solr_shards():
    search_url = 'https://esgf-node.llnl.gov/esg-search/search/' \
                 '?limit=0&format=application%2Fsolr%2Bjson'

    req = requests.get(search_url)
    js = json.loads(req.text)
    shards = js['responseHeader']['params']['shards']

    # Use the "files" part of the shards
    shards = shards.replace('solr/datasets','solr/files')

    return shards

def gen_script(dataset_list, output_dir):

    solr_shards = get_solr_shards()
    
    query_url = 'https://esgf-node.llnl.gov/solr/files/select' \
               '?q=*:*&wt=json&facet=true&fq=type:File&sort=id%20asc' \
               '&rows={rows}&shards={shards}&fq=dataset_id:{dataset_id}'

    file_list = []
    for dataset_id in dataset_list:
        # Query for the number of files
        query = query_url.format(rows=1, shards=solr_shards, dataset_id=dataset_id)
        req = requests.get(query)
        js = json.loads(req.text)
        numFiles = js['response']['numFound']

        # Query files
        query = query_url.format(rows=numFiles, shards=solr_shards, dataset_id=dataset_id)
        req = requests.get(query)
        js = json.loads(req.text)
        for file_info in js['response']['docs']:
            filename = file_info['title']
            checksum_type = file_info['checksum_type'][0]
            checksum = file_info['checksum'][0]
            for url in file_info['url']:
                url_split = url.split('|')
                if url_split[2] == "HTTPServer":
                    file_list.append(dict(filename=filename, 
                                          url=url_split[0], 
                                          checksum_type=checksum_type, 
                                          checksum=checksum))
                    break

    # Build wget script
    timestamp = datetime.datetime.now().strftime("%Y/%m/%d %H:%M:%S")

    script_loader = jinja2.FileSystemLoader(os.path.join(os.path.dirname(__file__),'wget-template.sh'))
    script_env = jinja2.Environment(loader=script_loader)
    script_template = script_env.get_template('')

    wget_script = script_template.render(timestamp=timestamp,
                                         datasets=dataset_list,
                                         files=file_list)


    script_filename = datetime.datetime.now().strftime("wget-%Y%m%d%H%M%S.sh")
    script_filepath = os.path.join(output_dir, script_filename)
    with open(script_filepath,'w') as f:
        print(wget_script, file=f)

def main():

    parser = argparse.ArgumentParser(description="Create wget script tables for ESGF files")
    parser.add_argument("--dataset", "-d", dest="dataset", type=str, action='append', 
                        help="Dataset ID", required=True)
    parser.add_argument("--output", "-o", dest="output", type=str, default=os.path.curdir, 
                        help="Output directory (default is current directory)")
    args = parser.parse_args()

    if not os.path.isdir(args.output):
        print("{} is not a directory. Exiting.".format(args.output))
        return

    gen_script(args.dataset, args.output)


if __name__ == '__main__':
	main()