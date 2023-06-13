def compute_table_link_depth(schema):
    categories= ["category1","category2","category3"]
    table_categories={category: [] for category in categories}
    for table in schema["schema"]["tables"]:
        local_table_links=0
        remote_table_links=0
        for column in table["columns"]:
            if column["type"]=="link":
                local_table_links+=1
                linked_table=column["link"]["table"]
                for iter_table in schema["schema"]["tables"]:
                    if iter_table["name"]==linked_table:
                        for iter_column in iter_table["columns"]:
                            if iter_column["type"]=="link":
                                remote_table_links+=1
        if local_table_links==0:
            #Category 1: no links in the table, can be indexed
            table_categories["category1"].append(table["name"])
        elif local_table_links>0 and remote_table_links==0:
            #Category 2: links in local table, no links in remote table. Can be indexed after remote table is indexed.
            table_categories["category2"].append(table["name"])
        else:
            #Category 3: links in local table, links in remote table. Cannot detemine link depth.
            table_categories["category3"].append(table["name"])
    return table_categories