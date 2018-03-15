
def shpinxQueryGenerator(limit,role,selections,table,max_cvid):
    selectionQuery = '''
                        SELECT
                            {selections}
                        FROM
                            {table}
                        WHERE
                            last_jb_role in ({role}) AND
                            cv_id > {max_cvid}
                        ORDER BY
                            cv_id ASC
                        LIMIT
                            {limit}
                        OPTION
                            max_matches = {limit}
    '''.format(
            table           = table,
            role            = role,
            max_cvid        = max_cvid,
            limit           = limit,
            selections      = selections
    )
    return selectionQuery
def pgQueryGenerator(ids):
    selectionQuery = '''
                        SELECT
                            skill_name
                        FROM
                            cv_skills
                        WHERE
                            cv_id in ({ids})
    '''.format(
            ids = ','.join([str(x) for x in ids])
    )
    return selectionQuery
def pgQueryGeneratorWithID(ids):
    selectionQuery = '''
                        SELECT
                            skill_name,cv_id::text
                        FROM
                            cv_skills
                        WHERE
                            cv_id in ({ids})
    '''.format(
            ids = ','.join([str(x) for x in ids])
    )
    return selectionQuery
def dataListStructure(dataList,ext):
    """ Convert list of tuble to list of lists """
    import itertools
    list_of_lists = [list(_item) for _item in dataList]
    if ext == 1:
        return (list(itertools.chain.from_iterable(list_of_lists)))
    else:
        return list_of_lists

def is_number(s):
    try:
        int(s)
        return True
    except ValueError:
        pass

def cluster(skillsDic):
    from collections import defaultdict

    clusters = defaultdict(list)
    for key, val in skillsDic.items():
        clusters[val].append(key)
    return clusters

def getSkillsById(skillsInfoList):
    from collections import Counter
    skillsInfoList = [x.lower() for x in skillsInfoList]
    return  Counter(skillsInfoList)


def getSkillsByIdClustred(skillsInfoList):
    print('\n=======================\nCOLLECTING IDS FROM SHPINX\n=======================')
    import itertools
    from progressbar import ProgressBar
    pbar = ProgressBar()
    print
    ids_list = []
    all_skills = []
    listItems = [skillsInfoList[x:x+2] for x in range(0, len(skillsInfoList), 2)]
    for i in pbar(listItems):
        ids_list.append(i[1])
    ids_list = list(set(ids_list))
    pbar = ProgressBar()
    print('\n=======================\nCLUSTRING SKILLS BY ID\n=======================')
    for id in pbar(ids_list):
        cv_skilsl = []
        for i in listItems:
            if i[1] == id:
                cv_skilsl.append(i[0])
        all_skills.append(cv_skilsl)
    return all_skills
