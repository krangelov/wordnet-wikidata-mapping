import sqlite3

query = """
    select ili,wikidata,group_concat(source),count(*),avg(score) 
    from (
       select ili,wikidata,1 as source,NULL as score from oewn_wikidata where ili is not NULL
       UNION
       select ili,wikidata,2 as source,NULL from gf_wikidata where ili is not NULL
       UNION 
       select t1.ili,t1.wikidata,3 as source,max(t1.score,t2.score)
             from yovisto_wikidata_kea_annotator t1
             inner join yovisto_wikidata_spotlight_annotator t2 on t1.ili = t2.ili
             where t1.wikidata = t2.wikidata
       UNION
       select t3.ili,wikidata,4 as source,NULL from babel_wn_30 t1 
             inner join wn_30_wn_31 t2 on t1.identifier  = t2.identifier
             inner join wn_all_synsets t3 on t3.Id = t2.id
       UNION
       select ili,wikidata,5 as source,score from yovisto_llm_as_a_judge)
    group by ili,wikidata order by count(*) desc, avg(score) desc;
    """

with sqlite3.connect("wordnet_wikidata_mapping.db") as con:
    with open("union.txt","w") as u, open("discarded.txt","w") as d:
        cur = con.cursor()
        dataset = ({},{})
        ambiguities = 0
        for fields in cur.execute(query):
            c = int(fields[3])
            if dataset[0].get(fields[0],0) > c:
                d.write(str(fields)+"\n")
            elif dataset[0].get(fields[0],0) == c:
                ambiguities += 1
            dataset[0][fields[0]] = c
            if dataset[1].get(fields[1],0) > c:
                d.write(str(fields)+"\n")
            elif dataset[1].get(fields[1],0) == c:
                ambiguities += 1
            dataset[1][fields[1]] = c
            u.write(str(fields)+"\n")
        print(ambiguities)
