#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""Program entry point"""

from __future__ import print_function

import argparse
import sys
import metadata
from dataConnectionManager.connectionManager import dbCon
from dataParser.dataFormatter import *
import skillsInfoConfig
class dataPreparing(object):
    """Read ids from sphinx and delver it to mongo"""
    def __init__(self):
        ####Shpinx query config####
        self.shpinxSelections       = ", ".join(skillsInfoConfig.SELECTIONS)
        self.shpinxRoles            = ", ".join(skillsInfoConfig.ROLES)
        self.shpinxLimit            = skillsInfoConfig.CUNCK_LIMIT
        self.shpinxTable            = skillsInfoConfig.TABLE
        self.shpinxService          = skillsInfoConfig.SHPINX_SERVICE
        self.shpinxMax_CVID         = skillsInfoConfig.MAX_CVID

    def getIds(self):
        """Returning cv ids in list structure"""
        query = shpinxQueryGenerator(        self.shpinxLimit,
                                             self.shpinxRoles,
                                             self.shpinxSelections,
                                             self.shpinxTable,
                                             self.shpinxMax_CVID
                                         )
        result = dataListStructure(dbCon.sphinxReadExe (query, self.shpinxService),1)
        return result


    def getSkills(self,op):
        """Returning cvs skills in list structure"""

        if op:
            query = pgQueryGenerator(self.getIds())
            result = getSkillsById(dataListStructure(dbCon.pgCoreReadExe (query, self.shpinxService),1))
            return(result)
        else:
            query = pgQueryGeneratorWithID(self.getIds())
            print("query DONE")
            result = getSkillsByIdClustred(dataListStructure(dbCon.pgCoreReadExe (query, self.shpinxService),1))
        clusterdSkills = [[j.lower() for j in i] for i in result]
        return clusterdSkills

    def writeToMongodb(self):
        from mongoOperations import mongoCRUD
        #mongoCRUD.uniqueBulckPost(self.getSkills(1))
        #mongoCRUD.getUpdateChunck(100000,self.getSkills(False),"occurrences")
        mongoCRUD.getUpdateChunck(100000,self.getSkills(False),"cluster")
        #print(mongoCRUD.getSkillHierarchy("java swing",100))
        #mongoCRUD.addEventsFrequency()
        #print(mongoCRUD.mongoTopFrequencyTerms(10))


def main(argv):
    """SERVICE entry point.

    :BAYT.COM
    :SKILLS PROJECT
    """
    author_strings = []
    for name, email in zip(metadata.authors, metadata.emails):
        author_strings.append('Author: {0} <{1}>'.format(name, email))

    epilog = '''
{project} {version}

{authors}
URL: <{url}>
'''.format(
        project=metadata.project,
        version=metadata.version,
        authors='\n'.join(author_strings),
        url=metadata.url)

    arg_parser = argparse.ArgumentParser(
        prog=argv[0],
        formatter_class=argparse.RawDescriptionHelpFormatter,
        description=metadata.description,
        epilog=epilog)
    arg_parser.add_argument(
        '-V', '--version',
        action='version',
        version='{0} {1}'.format(metadata.project, metadata.version))

    arg_parser.parse_args(args=argv[1:])

    print(epilog)

    return 0


def entry_point():
    ff = dataPreparing()
    #ff.getSkills(0)
    ff.writeToMongodb()
    raise SystemExit(main(sys.argv))



if __name__ == '__main__':
    entry_point()
