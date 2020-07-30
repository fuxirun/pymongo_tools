import collections
import bson

class Criteria():
    '''
    mongo 查询工具类
    '''
    def __init__(self,key = None,criteria_chain = None):
        self.criteria_chain = criteria_chain or []
        self.criteria = collections.OrderedDict()
        self.key = key or ''
        self.is_value = None
        if key:
            self.criteria_chain.append(self)


    @classmethod
    def where(self,key):
        return Criteria(key = key)

    def and_(self,key):
        return Criteria(key = key,criteria_chain=self.criteria_chain)

    def is_(self,obj):
        if self.is_value:
            raise Exception("Multiple 'is' values declared. You need to use 'and' with multiple criteria")
        if self._last_operate_was_not():
            raise Exception("Invalid query: 'not' can't be used with 'is' - use 'ne' instead.")
        self.is_value = obj
        return self

    def _last_operate_was_not(self):
        return self.criteria and "$not" == self.criteria[len(self.criteria) - 1]

    def ne_(self,obj):
        self.criteria['$ne'] = obj
        return self

    def lt_(self,obj):
        self.criteria['$lt'] = obj
        return self

    def lte_(self,obj):
        self.criteria['$lte'] = obj
        return self

    def gt_(self,obj):
        self.criteria['$gt'] = obj
        return self

    def gte_(self,obj):
        self.criteria['$gte'] = obj
        return self

    def in_(self,obj):
        if len(obj) > 1 and isinstance(obj[1], list):
            raise Exception("You can only pass in one argument of type")

        self.criteria['$in'] = obj
        return self

    def nin_(self,obj):
        self.criteria['$nin'] = obj
        return self

    def exists_(self,obj):
        self.criteria['$exists'] = obj
        return self

    def not_(self,obj = None):
        self.criteria['$not'] = obj
        return self

    def regex_(self,pattern,options = None):
        if not pattern:
            raise Exception("regex pattern nust not be null")
        if options:
            reg = bson.regex.Regex(pattern,options)
        else:
            reg = bson.regex.Regex(pattern)
        if self._last_operate_was_not():
            return self.not_(reg)

        self.is_value = reg
        return self

    def or_operator(self,*criterias):
        statement_list = []
        for criteria in criterias:
            statement_list.extend(self._ceate_criteria_list(criteria))

        return self._register_criteria_chain_element(Criteria(key='$or').is_(statement_list))

    def nor_operator(self,*criterias):
        statement_list = []
        for criteria in criterias:
            statement_list.append(self._ceate_criteria_list(criteria))
        return self._register_criteria_chain_element(Criteria(key='$nor').is_(statement_list))

    def and_operator(self,criterias):
        statement_list = []
        for criteria in criterias:
            statement_list.append(self._ceate_criteria_list(criteria))
        return self._register_criteria_chain_element(Criteria(key='$and').is_(statement_list))

    def _register_criteria_chain_element(self,criteria):
        if self._last_operate_was_not():
            raise Exception("operator $not is not allowed around criteria chain element: " + str(criteria.get_criteria_object()))
        else:
            self.criteria_chain.append(criteria)

        return self

    def _ceate_criteria_list(self,criterias):
        statement_list = []
        if isinstance(criterias,Criteria):
            statement_list.append(criterias.get_criteria_object())
        else:
            for c in criterias:
                statement_list.append(c.get_criteria_object())
        return statement_list

    def get_criteria_object(self):
        if len(self.criteria_chain) == 1:
            return self.criteria_chain[0].get_single_criteria_object()
        elif len(self.criteria_chain) == 0 and len(self.criteria) > 0:
            return self.get_single_criteria_object()
        else:
            query_statement = {}
            for criteria in self.criteria_chain:
                statement = criteria.get_single_criteria_object()
                for key,value in statement.items():
                    self._set_value(query_statement,key,value)
            return query_statement

    def get_single_criteria_object(self):
        not_flag = False
        statement = {}
        for key,value in self.criteria.items():
            if not_flag:
                statement['$not'] = {key,value}
                not_flag = False
            else:
                if not value and '$not' == key:
                    not_flag = True
                else:
                    statement[key] = value

        if not self.key:
            if not_flag:
                return {'$not': statement}
            return statement

        query_statement = {}

        if self.is_value is not None:
           query_statement[self.key] = self.is_value
           query_statement.update(statement)
        else:
            query_statement[self.key] = statement

        return query_statement

    def _set_value(self,query_statement,key,value):
        existing = query_statement.get(key)
        if existing:
            raise Exception("Due to limitations of the com.mongodb.BasicDBObject, "
					+ "you can't add a second '" + str(key) + "' expression specified as '" + str(key) + " : " + str(value) + "'. "
					+ "Criteria already contains '" + str(key) + " : " + str(existing) + "'.")
        else:
            query_statement[key] = value


if __name__ == '__main__':
    criteria = Criteria.where("a").is_("b").and_('c').is_('d')
    criteria.or_operator(Criteria.where("x").gt_("g").lt_('h'),Criteria.where("x").is_("y"))
    print(criteria.get_criteria_object())
    print(isinstance("app", str))