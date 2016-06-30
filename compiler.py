# -*- coding: utf-8 -*- 
                                                     
from pyparsing import Literal, CaselessLiteral, Word, Upcase, delimitedList, Optional, \
    Combine, Group, alphas, nums, alphanums, ParseException, Forward, oneOf, quotedString, \
    ZeroOrMore, restOfLine, Keyword 
def flatten(foo):
    for x in foo:
        if hasattr(x, '__iter__'):
            for y in flatten(x):
                yield y
        else:
            yield x


def test( str ):  
    print str,"Translated as:"
    try:
        tokens = simpleSQL.parseString( str )
        tokens1 = tokens.asList()
        print "tokens = ",        tokens
        print "tokens.columns =", tokens.columns
        print "tokens.tables =",  tokens.tables
        print "tokens.where =", tokens.where
        print "tokens.nestedcolumns =", tokens.nestedcolumns
        print "tokens.nestedtables =",  tokens.nestedtables
        print "tokens.nestedwhere =", tokens.nestedwhere
        print "tokens.groupby =", tokens.groupby
        print "tokens.having =", tokens.having
        print "tokens.opercloumns =", tokens.opercolumns
        print "tokens.opertables =", tokens.opertables
        print "tokens.operwhere =", tokens.operwhere
        
        groupbyvals=list()
        havingvals=list()
        
        wherevals = list()
        wherevals = tokens.where.asList()
        tablevals = tokens.tables.asList()
        print "the tablevvals are:",tablevals
        tablevalsloop = tokens.tables.asList()
        if 'GROUPBY' in tokens1:
            groupbyvals=tokens.groupby.asList()
            print "groupby length:",len(groupbyvals)
        if 'HAVING' in tokens1:
            havingvals=tokens.having.asList()
            print "having length:",len(havingvals)
        
        print tablevals
        aliasloop = list()
        x=len(tablevals)
        for x in range (0, x):
           aliasloop.append(tablevals[x])
           if tablevals[x]=='AS':
              aliasloop.remove(tablevals[x])
              aliasloop.remove(tablevals[x-1])
              x=x+2
        tablevals=aliasloop
        tablevals = 'X'.join(tablevals)
        print " wherevals are:",wherevals
        if len(wherevals)>1:
            wherevals = flatten(wherevals)
            print " wherevals are:",wherevals
            wherevals = ' '.join(wherevals)
            print " wherevals are:",wherevals
            wherevals = wherevals.replace("where","")
            print " wherevals are:",wherevals
        if len(groupbyvals)>=1:
            groupbyvals = flatten(groupbyvals)
            groupbyvals = ' '.join(groupbyvals)
            groupbyvals = groupbyvals.replace("groupby","")
            print "flannened groupby:", groupbyvals
        print " length of havingvals", len(havingvals)
        if len(havingvals)>1:
            havingvals = flatten(havingvals)
            print "flatten step1: having:",havingvals
            havingvals = ' '.join(havingvals)
            havingvals = havingvals.replace("having","")
            print "having groupby:", havingvals
        print "Groupby vals are :",groupbyvals
        print "Having vals are:",havingvals
        projectvals = tokens.columns
        print "projectals are:",projectvals
        y=len(projectvals)
        print "lenght of projvals",y
        projalias=list()
        for y in range(0, y):
            projalias.append(projectvals[y])
            if projectvals[y]=='AS' or projectvals[y]=='as':
               projalias.remove(projectvals[y])
               projalias.remove(projectvals[y-1])
               y=y+2  
        projectvals=projalias
        if len(projectvals)==1: 
            projectvals = projectvals[0]
        if len(projectvals)==2: 
            projectvals = projectvals[0],projectvals[1]
        if len(projectvals)==3:
            projectvals = projectvals[0],projectvals[1],projectvals[2]

        if len(groupbyvals) >= 1 and len(havingvals) >= 1:
           relalg = "PROJECT", projectvals, "(","SELECT",wherevals,"SORT",groupbyvals,"HAVING",havingvals,"(",tablevals,")",")"
           relalg = flatten(relalg)
           relalg = ' '.join(relalg)
        elif len(groupbyvals) >= 1:
           relalg = "PROJECT", projectvals, "(","SELECT",wherevals,"SORT",groupbyvals,"(",tablevals,")",")"
           relalg = flatten(relalg)
           relalg = ' '.join(relalg)
        else:   
           relalg = "PROJECT", projectvals , "(" , "SELECT" , wherevals , "(" , tablevals , ")" , ")"
           relalg = flatten(relalg)
           relalg = ' '.join( relalg)
        print relalg

        if 'union' in tokens1 or 'intersect' in tokens1 or 'except' in tokens1: 
           wherevals1 = list()
           wherevals1 = tokens.operwhere.asList()
           tablevals1 = tokens.opertables.asList()
           tablevalsloop1 = tokens.opertables.asList()
           node7 = []
           node8 = []
           print tablevals1
           aliasloop1 = list()
           if 'AS' not in tablevals1:
               node6 = tablevals1[0]
               if len(tablevals1)>1:
                   node7 = tablevals1[1]
           if len(tablevals1)>1:
               while 'AS' in tablevalsloop1:
                   pos1 = tablevalsloop1.index('AS')
                   aliasloop1.append(tablevalsloop1[pos1+1])
                   tablevalsloop1 = tablevalsloop1[pos1+1:]
               print aliasloop1
               if 'AS' in tablevals1:
                   node6 = aliasloop1[0]
                   if len(aliasloop1)>1:
                       node7 = aliasloop1[1]
                   tablevals1 = aliasloop1
                   if len(aliasloop1)>2:
                       tablevals1 = aliasloop1[0] , aliasloop1[1] , "(" + aliasloop1[2] + ")"
                       node8 = aliasloop1[2]
           tablevals1 = 'X'.join(tablevals1)
           if len(wherevals1)>1:
               wherevals1 = flatten(wherevals1)
               wherevals1 = ' '.join(wherevals1)
               wherevals1 = wherevals1.replace("where","")
           projectvals1 = tokens.opercolumns 
           if len(projectvals1)==1:
               projectvals1 = projectvals1[0]
           if len(projectvals1)==2:
               projectvals1 = projectvals1[0],projectvals1[1]
           if len(projectvals1)==3:
               projectvals1 = projectvals1[0],projectvals1[1],projectvals1[2]
           relalg1 = "PROJECT", projectvals1 , "(" , "SELECT" , wherevals1 , "(" , tablevals1 , ")" , ")"
           relalg1 = flatten(relalg1)
           relalg1 = ' '.join( relalg1)
           print relalg1 
         
        

        if 'union' in tokens1:
            print relalg+"   UNION   "+relalg1 
        elif 'intersect' in tokens1:
            print relalg+"   INTERSECT   "+relalg1
        elif 'except' in tokens1:
            print relalg+"   MINUS   "+relalg1
        
    except ParseException, err:
        print " "*err.loc + "^\n" + err.msg
        print err
    print


    

# define SQL tokens
selectStmt = Forward()
condition = Forward()
treegram = Forward()
comps = Forward()
selectToken = Keyword("select", caseless=True)
fromToken   = Keyword("from", caseless=True)
astoken  = Keyword("AS", caseless=True)
groupToken = Keyword("GROUPBY",caseless=True)
havingToken = Keyword("HAVING",caseless=True)

ident          = Word( alphas, alphanums + "_$" ).setName("identifier")
columnName     = Upcase( delimitedList( ident, ".", combine=True ) )
columnNameList = Group( delimitedList( columnName ) )
columnNameList1 = Group( delimitedList( columnName ) )
groupNameList  = Group( delimitedList( columnName) )
tableName      = Upcase( delimitedList( ident, ".", combine=True ) )
tableName2      = Upcase( delimitedList( ident, ".", combine=True ) )
tableAlias  = tableName + astoken + tableName2
tableNameList  = Group( delimitedList( tableAlias | tableName ) )

whereExpression = Forward()
and_ = Keyword("and", caseless=True)
or_ = Keyword("or", caseless=True)
in_ = Keyword("in", caseless=True)


E = CaselessLiteral("E")
binop = oneOf("= != < > >= <= eq ne lt le gt ge", caseless=True)
oper = oneOf("intersect union except contains",caseless=True)
oper1 = oneOf("count min max avg",caseless=True)
asoper=oneOf("as",caseless=True)

arithSign = Word("+-",exact=1)
realNum = Combine( Optional(arithSign) + ( Word( nums ) + "." + Optional( Word(nums) )  |
                                                         ( "." + Word(nums) ) ) + 
            Optional( E + Optional(arithSign) + Word(nums) ) )
intNum = Combine( Optional(arithSign) + Word( nums ) + 
            Optional( E + Optional("+") + Word(nums) ) )

columnRval = realNum | intNum | quotedString | columnName   # need to add support for alg expressions
whereCondition = ZeroOrMore(
    ( columnName + binop + columnRval ) | 
    ( columnName + in_ + "(" + delimitedList( columnRval ) + ")" ) |
    ( columnName + in_ + "(" + ZeroOrMore(selectToken + 
                   ( '*' | columnNameList ).setResultsName( "nestedcolumns" ) + 
                   fromToken + 
                   tableNameList.setResultsName( "nestedtables" ) + 
                   Optional( ZeroOrMore( CaselessLiteral("where") + whereExpression ), "" ).setResultsName("nestedwhere")) + ")" ) |
    ( "(" + whereExpression + ")" ) |
    ( columnName + in_ + "(" + ZeroOrMore(selectToken + 
                   ( '*' | columnNameList ).setResultsName( "nestedcolumns" ) + 
                   fromToken + 
                   tableNameList.setResultsName( "nestedtables" ) + 
                   Optional( ZeroOrMore( CaselessLiteral("where") + whereExpression ), "" ).setResultsName("nestedwhere")) + ")" ) |
    ( "(" + whereExpression + ")" ) + Optional( oper + "(" + ZeroOrMore(selectToken + 
                   ( '*' | columnNameList ).setResultsName( "nestedcolumns" ) + 
                   fromToken + 
                   tableNameList.setResultsName( "nestedtables" ) + 
                   Optional( ZeroOrMore( CaselessLiteral("where") + whereExpression ), "" ).setResultsName("nestedwhere")) + ")" ) |
    ( "(" + whereExpression + ")" )
    )
whereExpression << whereCondition + ZeroOrMore( ( and_ | or_ ) + whereExpression ) 

# define the grammar
selectStmt      << ( selectToken + 
                   ( ('*' | columnNameList | (oper1 +"("+ ( '*' | columnNameList )+")") )+Optional(asoper + columnRval)).setResultsName("columns" ) + 
                   fromToken + 
                   tableNameList.setResultsName( "tables" ) + 
                   Optional( ZeroOrMore( CaselessLiteral("where") + whereExpression ), "" ).setResultsName("where") +
                   Optional( groupToken + ( '*' | groupNameList ).setResultsName("groupby"))+
                   Optional( havingToken + (((oper1 + ( '*' | columnNameList ) | (columnNameList)))+binop+columnRval).setResultsName("having"))+
                   Optional (oper) + Optional (selectToken+ ( '*' | columnNameList1 ).setResultsName( "opercolumns" ) )+
                   Optional ( fromToken + tableNameList.setResultsName( "opertables" ) )+ 
                   Optional( ZeroOrMore( CaselessLiteral("where") + whereExpression ), "" ).setResultsName("operwhere"))     

    

condition << ( ZeroOrMore (columnName + binop + columnName) )

simpleSQL = selectStmt
conditionstmt = condition



# define Oracle comment format, and ignore them
oracleSqlComment = "--" + restOfLine
simpleSQL.ignore( oracleSqlComment )

#test("select S.sid as SID from Sailors AS S, Reserves AS R, Boats AS B where S.sid=R.sid AND R.bid=B.bid AND B.color=red except select S2.sid from Sailors AS S2, Reserves AS R2, Boats AS B2 where S2.sid=R2.sid AND R2.bid=B2.bid AND B2.color=green")

#test("select S.sname from Sailors AS S, Reserves AS R where S.sid=R.sid and R.bid=103")
#test("Select S.sname from Sailors AS S,Reserves AS R where S.sid=R.sid and R.bid=103")
#test("SELECT S.sname FROM Sailors AS S, Reserves AS R WHERE R.sid = S.sid AND R.bid = 100 AND R.rating > 5 AND R.day ='8/9/09'" )

#test("SELECT S.sname FROM Sailors AS S, Reserves AS R, Boats AS B WHERE S.sid=R.sid AND R.bid=B.bid AND B.color=red")
#test("SELECT sname FROM Sailors, Boats, Reserves WHERE Sailors.sid=Reserves.sid AND Reserves.bid=Boats.bid AND Boats.color=red UNION SELECT sname FROM Sailors, Boats, Reserves WHERE Sailors.sid=Reserves.sid AND Reserves.bid=Boats.bid AND Boats.color=green")

#test("SELECT sname FROM Sailors, Boats, Reserves WHERE Sailors.sid=Reserves.sid AND Reserves.bid=Boats.bid AND Boats.color=red INTERSECT SELECT sname FROM Sailors, Boats, Reserves WHERE Sailors.sid=Reserves.sid AND Reserves.bid=Boats.bid AND Boats.color=green")

#test("SELECT S.sid FROM Sailors AS S, Reserves AS R, Boats AS B WHERE S.sid=R.sid AND R.bid=B.bid AND B.color=red EXCEPT SELECT S2.sid FROM Sailors AS S2, Reserves AS R2, Boats AS B2 WHERE S2.sid=R2.sid AND R2.bid=B2.bid AND B.2color=green")

#test("SELECT S.sname FROM Sailors AS S WHERE S.sid IN ( SELECT R.sid FROM Reserve AS R WHERE R.bid = 103)")

#test("SELECT B.bid,reservationcount FROM Boats AS B, Reserves AS R WHERE R.bid=B.bid AND B.color = red GROUPBY B.bid")

#test("SELECT B.bid, resere FROM Boats AS B, Reserves AS R WHERE R.bid=B.bid AND B.color = red GROUPBY B.bid HAVING count * > 1")

test("SELECT S.rating, average FROM Sailors AS S WHERE S.age > 18 GROUPBY S.rating HAVING Count * > 1 ")
#test("SELECT S.sname FROM Sailors AS S, Reserves AS R, Boats AS B WHERE S.sid=R.sid AND R.bid=B.bid AND B.color=red ")
#test("SELECT S.sname FROM Sailors AS S, Reserves, Boats WHERE S.sid=R.sid AND R.bid=B.bid AND B.color=red")
