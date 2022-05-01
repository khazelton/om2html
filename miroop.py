fullersr, sfullerton@wisc.edu

#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Functions that contain logic specific to the MIRO ontology
"""
import time

from pyom.ontology import om2_1
from pyom.ontology import miro
from pyom.uuid import uuid1


class MiroSpecException(Exception):
    def __init__(self, *argv):
        Exception.__init__(self, *argv)


class NotAllowedConstruct(Exception):
    def __init__(self, *argv):
        Exception.__init__(self, *argv)

# -----------------------------------------------------------------------------
# New message


def new_message(oper, sender, receiver=None, args=None):
    """
    creates a new message graph

    :param oper: A miro operation or a list of miro operations that are to be
        placed in the message
    :param sender: The sender of the message, there *MUST* to be a sender
    :param receiver: The receivers of the message, there does not have to be
        a receiver specified
    :param args: Other properties that a message may have
    """
    msg = om2_1.Message()
    if oper:
        msg["body"] = oper
    msg["source"] = sender
    if receiver:
        msg["receiver"] = receiver
    msg["createTime"] = time.strftime("%FT%T")
    try:
        msg["mid"] = args["mid"]
        del args["mid"]
    except (KeyError, TypeError):
        msg["mid"] = "%s" % (uuid1(), )

    if args:
        for key, val in args.items():
            if sender and key == "sender":  # don't overwrite
                continue
            if receiver and key == "receiver":
                continue
            if key == "body":
                continue

            msg[key] = val

    return msg

# The combinations of properties that are allowed
ALLOWEDCONSTRUCTS = ["MIRO", "MIR", "MI", "MR", "O", "MIO", "MRO", "MO", "M"]


def _verify_about(oper, parts):
    for part in parts:
        if part in oper:
            if isinstance(oper[part][0], basestring):
                continue
            try:
                _ = oper[part][0].about
            except AttributeError:
                oper[part][0].about = ""


def new_body(args):
    """Creates and returns a new miro operation graph.

    - `args`: The different parts of a miro operation, represented as a
        dictionary. The Keys comes from the set "M", "I", "R", "O".
    """
    oper = miro.MIRO()
    optype = ""

    for (short_name, long_name) in [("M", "match"), ("I", "insert"),
                                    ("R", "replace"), ("O", "otherwiseInsert")]:
        if short_name in args:
            oper[long_name] = args[short_name]
            optype += short_name
        elif long_name in args:
            oper[long_name] = args[long_name]
            optype += short_name

    if optype not in ALLOWEDCONSTRUCTS:
        raise NotAllowedConstruct("%s not an allowed MIRO construct" %
                                  (optype, ))

    _verify_about(oper, ["match", "insert", "replace", "otherwiseInsert"])
    if "match" in oper:
        if not oper["match"]:
            match = None
            for typ in ["insert", "replace", "otherwiseInsert"]:
                if typ in oper:
                    match = oper[typ][0].__class__()
                    match.about = oper[typ][0].about
                    oper[typ][0].about = ""
                    break
            try:
                oper["match"] = match
            except NameError:
                raise MiroSpecException("Missing match specification")

    if "O" in optype and "M" in optype:
        if oper["otherwiseInsert"][0].about == oper["match"][0].about:
            oper["otherwiseInsert"][0].about = ""

    if "M" in optype:
        if "R" in optype:
            if oper["replace"][0].about and \
                    oper["replace"][0] == oper["match"][0].about:
                oper["replace"][0].about = ""
        if "I" in optype:
            if oper["insert"][0].about:
                oper["insert"][0].about = ""

    return oper


def new_add_body(obj):
    """
    Creates and returns an *otherwiseInsert* type of operation, an
    *otherwiseInsert* only operation is roughly equal to an *Add* operation

    - `object`: The object that is to be added, an instance of
        rdfmarshal.Base .
    """
    return new_body({"O": obj})


def new_add_message(obj, sender, receiver=None, args=None):
    return new_message(new_add_body(obj), sender, receiver, args)


def new_delete_body(obj):
    """
    Creates and returns a *Match* type of operation, a *Match* only
    operation is equal to an *Delete* operation.
    If object is only a URIRef or a instance of ``rdfmarshal.instance.Base``
    but without any property specifications then the delete will effect the
    whole object.
    If object is a instance of ``rdfmarshal.instance.Base`` with properties
    defined then those properties will be effected.

    parameters:

    - object: The object that is to be deleted
    """
    return new_body({"M": obj})


def new_delete_message(obj, sender, receiver=None, args=None):
    return new_message(new_delete_body(obj), sender, receiver, args)


def new_insert_body(insert, match=None):
    """
    Creates and resturns a *Match*+*Insert* type of operation, an *Match*+
    *Insert* operation is roughly equal to an *ModifyAdd* operation.
    With the difference that the *Match* object might not be the same as the
    *Insert* one.

    - `insert`: The object that is to be added
    - `to`: The object that has to be in the repository ( the match object )
        for the operation to be performed.
    """
    if not match:
        match = insert.__class__()
        match.about = insert.about
        insert.about = None
    return new_body({"I": insert, "M": match})


def new_insert_message(insert, sender, receiver=None, match=None, args=None):
    return new_message(new_insert_body(insert, match), sender, receiver, args)


def new_insert_or_add_body(match, insert, add):
    """
    Creates a *Match* and *Insert* or *otherwiseInsert* type of operation.
    This is an operation that will insert the *Insert* part into the
    repository if the *Match* pattern appears in the repositorty otherwise
    it will add the *otherwiseInsert* part to the repository.

    - `to`: The *match* pattern. If this object exists then an insert
        operation will be performed otherwise it will be an add operation.
    - `insert`: The object that is to be inserted
    - `add`: The object that is to be added
    """
    return new_body({"I": insert, "M": match, "O": add})


def new_insert_or_add_message(match, insert, add, sender, receiver=None,
                              args=None):
    return new_message(new_insert_or_add_body(match, insert, add), sender,
                       receiver, args)


def new_replace_body(match, replace):
    """
    Creates a *Match*+*Replace* type of operation, an *Match*+*Replace*
    operation is roughly equal to an *Modify* operation.

    - object: This object defines what can be changed.
    - new: This object defines to what.
    """
    return new_body({"R": replace, "M": match})


def new_replace_message(match, replace, sender, receiver=None, args=None):
    return new_message(new_replace_body(match, replace), sender, receiver, args)


def new_replace_or_add_body(match, replace, add):
    """
    Creates a *Match* and *Replace* or *otherwiseInsert* type of operation.
    This is an operation that will if *Match* matches replace that part
    with what's defined in the *Replace* otherwise
    it will add the *otherwiseInsert* part to the repository.

    - `match`: The *match* pattern. If this object exists then an replace
        operation will be performed otherwise it will be an add operation.
    - `replace`: The object that is to be inserted
    - `add`: The object that is to be added
    """
    return new_body({"R": replace, "M": match, "O": add})


def new_replace_or_add_message(match, replace, add, sender, receiver=None,
                               args=None):
    return new_message(new_replace_or_add_body(match, replace, add), sender,
                       receiver, args)

# ----------------------------------------------------------------------------


def operation(oper):
    """
    The type of miro operation, returns s string of the form
    ["M"]["I"]["R"]["O"]

    - `op`: An operation
    """
    res = ""
    if "match" in oper:
        res += "M"
    if "insert" in oper:
        res += "I"
    if "replace" in oper:
        res += "R"
    if "otherwiseInsert" in oper:
        res += "O"
    return res


def addOperation(oper):
    return is_add_operation(oper)


def is_add_operation(oper):
    """ Answers the question: Is this an add operation ?
    Returns a boolean: True or False"""
    if len(oper) == 1 and "otherwiseInsert" in oper:
        return True
    else:
        return False


def deleteOperation(oper):
    return is_delete_operation(oper)


def is_delete_operation(oper):
    """ Answers the question: Is this an delete operation ?
    Returns a boolean: True or False"""
    if len(oper) == 1 and "match" in oper:
        return True
    else:
        return False


def is_move_operation(oper):
    """
    A move operation is defined as a "MR" operation where the "M" and
    "R" objects has different nonzero uriref's but no properties.
    """
    if len(oper) == 2 and "match" in oper and "replace" in oper:
        if oper["match"][0].about and oper["replace"][0].about and \
                len(oper["match"][0]) == 0 and len(
                oper["replace"][0]) == 0:
            return True
    return False

# ----------------------------------------------------------------------------


def about(oper):
    """
    Who is this operation affecting, returns a URIRef

    - `op`: The operation
    """
    opt = operation(oper)
    res = []
    if "M" in opt:
        res.append(oper["match"][0].about)
    if "R" in opt and oper["replace"][0].about:
        res.append(oper["replace"][0].about)
    if "O" in opt:
        res.append(oper["otherwiseInsert"][0].about)
    return res

# ----------------------------------------------------------------------------


def verify(oper):
    """
    Verify that the operation is OK
    This is just a start, many more checks could be done.
    """
    oper.within_restrictions()

    if operation(oper) not in ALLOWEDCONSTRUCTS:
        raise NotAllowedConstruct("Not an allowed MIRO construct '%s'" %
                                  operation(oper))

    return True

# ----------------------------------------------------------------------------


def remove_object_type(oper):
    """Is this operation aiming to remove a whole object from the repository?
    """
    if operation(oper) == "M":
        if len(oper["match"][0]) == 0:
            return True
    return False


def is_rename_operation(oper):
    """Is this a rename operation ? A rename operation is defined as an
    operation that doesn't change any aspect of the object except its 'name'.
    The name is a URI referens.
    """
    if operation(oper) == "MR":
        match = oper["match"][0]
        replace = oper["replace"][0]
        if len(match) == 0 and match.about and \
                len(replace) == 0 and replace.about:
            return True

    return False

# ----------------------------------------------------------------------------


__test__ = {"a": """
>>> import pyom.ontology.prim_3 as prim
>>> p = prim.Person()
>>> p["givenName"] = "Derek"
>>> p.about = "http://www.mlb.com/players/jeter_derek/"
>>> op = new_body({"O":p})
>>> print op.graph()
<?xml version="1.0" encoding="utf-8"?>
<rdf:RDF
  xmlns:_4='http://www.openmetadir.org/om2/miro.owl#'
  xmlns:rdf='http://www.w3.org/1999/02/22-rdf-syntax-ns#'
  xmlns:_3='http://www.openmetadir.org/om2/prim-3.owl#'
>
  <_4:MIRO>
    <_4:otherwiseInsert>
      <_3:Person rdf:about="http://www.mlb.com/players/jeter_derek/">
        <_3:givenName>Derek</_3:givenName>
      </_3:Person>
    </_4:otherwiseInsert>
  </_4:MIRO>
</rdf:RDF>
>>> op = new_body({"O":p, "M":None})
>>> op.keys()
['otherwiseInsert', 'match']
>>> op["match"][0].about
u'http://www.mlb.com/players/jeter_derek/'
>>> op["otherwiseInsert"][0].about
u''
>>> op["otherwiseInsert"][0].dumps()
{u'http://www.openmetadir.org/om2/prim-3.owl#Person': {u'': {'givenName': [
'Derek']}}}
""", "b": """
>>> import pyom.ontology.prim_3 as prim
>>> p = prim.Person()
>>> p["givenName"] = "Derek"
>>> p.about = "http://www.mlb.com/players/jeter_derek/"
>>> op = new_delete_body(p)
>>> print op.graph()
<?xml version="1.0" encoding="utf-8"?>
<rdf:RDF
  xmlns:_4='http://www.openmetadir.org/om2/miro.owl#'
  xmlns:rdf='http://www.w3.org/1999/02/22-rdf-syntax-ns#'
  xmlns:_3='http://www.openmetadir.org/om2/prim-3.owl#'
>
  <_4:MIRO>
    <_4:match>
      <_3:Person rdf:about="http://www.mlb.com/players/jeter_derek/">
        <_3:givenName>Derek</_3:givenName>
      </_3:Person>
    </_4:match>
  </_4:MIRO>
</rdf:RDF>
""",
            "c": """
>>> import pyom.ontology.prim_3 as prim
>>> p = prim.Person()
>>> p.about = "http://www.mlb.com/players#jeter_derek"
>>> op = new_body({"M":p})
>>> print removeObjectType(op)
True
""",
            "d": """
>>> import pyom.ontology.prim_3 as prim
>>> p = prim.Person()
>>> p.about = "http://www.mlb.com/players#jeter_derek"
>>> p["givenName"] = "Derek"
>>> op = new_body({"M":p})
>>> print removeObjectType(op)
False
""",
            "e": """
>>> import pyom.ontology.prim_3 as prim
>>> from pyom.ontology import miro
>>> p1 = prim.Person()
>>> p1.about = "http://www.mlb.com/nyy/player#jeter_derek"
>>> p2 = prim.Person()
>>> p2.about = "http://www.mlb.com/oak/player#jeter_derek"
>>> b = new_body({"M":p1, "R":p2})
>>> isRenameOperation(b)
True
>>> del p2.about
>>> p2["surName"] = "Jeter"
>>> b = new_body({"M":p1, "R":p2})
>>> isRenameOperation(b)
False
"""}

if __name__ == '__main__':
    import doctest

    doctest.testmod()
