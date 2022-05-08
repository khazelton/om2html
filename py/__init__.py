#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
#  __init__.py
#  om2api
#
#  Created by Roland Hedberg on 8/28/09.
#  Copyright (c) 2009 Umeå Universitet. All rights reserved.
#

"""
A OM2 API implementation.

"""

try:
    import om2api.sparql as sparql
except ImportError:
    import sparql
    
import httplib2
import urllib
import socket

import rdfmarshal.instance as instance
import rdfmarshal.base as base
from rdfmarshal import parse 
from urlparse import urlparse
from rdflib import StringInputSource, Literal, URIRef
from rdflib.syntax.xml_names import split_uri
from pyom import twist
from pyom import miroop

from pyom.log import log_info, log_debug, log_error, StdOutLogger
from pyom.log import error_description, safe_str

#class Entity(instance.Base):
#     """Base does everything Entity is supposed to do."""

class UpdateError(Exception):
    pass
    
class WrongTypeOfObject(Exception):
    pass

def fullname_from_ontology( ontology, ref):
    """ Return the full name for property or list of properties given
    that they belong to a specific ontology.
    
    :param ontology: The python ontology module
    :param ref: A property name or a list of property names
    :return: The full name for the given property/properties.
    """
    
    if isinstance(ref, list):
        return [str(ontology.PropertyFactory(p).type) \
                    for p in ref]
    elif isinstance(ref, dict):
        return dict([(str(ontology.PropertyFactory(p).type), v) \
                    for p, v in ref.items()])
    else:
        return str(ontology.PropertyFactory(ref).type)

def name_arr(props, ont):
    #may throw an exception
    return [split_uri(unicode(fullname_from_ontology(ont, prop))) \
                for prop in props]

def unfurl(obj):
    """ Will from a object instance pick out some sailient facts 
    
    :param obj: The object instance; a dictionary as returned by a 
        search.
    :return: A 3-tuple containing the type of the object, its identifier and
            the properties and their values
    """
    if not isinstance(obj,dict):
        raise ValueError("object must be dictionary")
        
    if len( obj.keys() ) != 1:
        raise ValueError(
            "Wrong number of keys in the dictionary [%s]" % (obj.keys(),))
    
    object_type = obj.keys()[0]
    
    if isinstance(obj[object_type],list) or isinstance(obj[object_type],tuple):
        try:
            (uriref,ava) = obj[object_type]
            if ava == None:
                ava = {}
        except ValueError:
            uriref = obj[object_type][0]
            ava = {}
    elif isinstance(obj[object_type],basestring):
        uriref = obj[object_type]
        ava = {}
    else:
        raise ValueError("Wrong value format")

    return (object_type, uriref, ava)

def dict_join(dic0,dic1):
    for key,values in dic1.items():
        if key in dic0:
            for val in values:
                if val in dic0[key]:
                    pass
                else:
                    dic0[key].append(val)
        else:
            dic0[key] = values
    return dic0
    
def set_lang(val):
    if isinstance(val,basestring) and ";lang-" in val:
        str,lang = val.split(";lang-")
        return Literal(str,lang=lang.lower())
    else:
        return val

class NextHop(object):
    """ Base class for protocol specific implementation of the nexthop """
    def __init__(self, path):
        self.path = path
        
    def write(self, data):
        pass 
        
class _Uds(NextHop):
    """ For writing to a Unix Domain Socket """
    def __init__(self, path):
        self.server = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        NextHop.__init__(self, path[4:])

    def write(self, data):
        self.server.connect(self.path)
        self.server.send(data)
        data = self.server.recv(1024)
        data.strip("\r\n")
        self.server.close()
        if data == "OK":
            return True
        else:
            return False

class _Http(NextHop):
    """ For writing to a HTTP server using PUT """
    def __init__(self, path, key_file=None, cert_file=None):
        NextHop.__init__(self, path)
        self.server = httplib2.Http()
        if key_file:
            self.server.add_certificate(key_file, cert_file, "")
        
    def write(self, data):
        (response, content) = self.server.request(self.path, "PUT", data)
        if response.status == 200:
            return True
        else:
            return False

class _File(NextHop):
    """ For writing to a file """
    def __init__(self, path):
        # path starts with 'file:///'
        NextHop.__init__(self, path[8:])
        
    def write(self, data):
        fil = open(self.path, "a+")
        fil.write(data)
        fil.write("\n")
        fil.flush()
        fil.close()
        return True
        

class OM2(object):
    """An implementation of a om2 api, something that to an application
        provides something that might look like a database but which in fact
        is a interface to a OM2 system.

    :param neorepo: where the NeoRepo can be found 
    :param nexthop: which node in the OM2 cloud to send messages to 
    :param dnssrv: which DNS server to use to find the receiver in the 
        OM2 cloud.
    :param ontology: which ontologies to use for constructing or parsing
    :param ontpath: where the ontologies can be found if not in the
        normal places
    :param sender: Who to send as
    :param receiver: Default receivers of the messages 
    :param key_file: If https this is the key to us
    :param cert_file: Certificates to validate the other side
    :param debug: To turn on debugging
    :param verbose: Make some more verbose logging
    :param log: Logger to use
    """
    
    def __init__(self, neorepo=None, nexthop=None, dnssrv=None, ontology=[],
        ontpath=[], sender="someone@example.com", receiver=[], 
        key_file=None, cert_file=None, debug=0, verbose=False, log=None):
        if neorepo:
            if not neorepo.endswith("/"):
                neorepo += "/"
            self.neorepo = httplib2.Http()
            self.sparql = sparql.Sparql(self.neorepo, neorepo+"sparql/",log)
            self.lookup_path = neorepo+"lookup/"
            if key_file:
                self.neorepo.add_certificate(key_file, cert_file, "")

        if nexthop:
            if nexthop.startswith("http"):
                self.nexthop = _Http(nexthop, key_file, cert_file)
            elif nexthop.startswith("uds:"):
                self.nexthop = _Uds(nexthop)
            elif nexthop.startswith("file:///"):
                self.nexthop = _File(nexthop)
        else:
            self.nexthop = None
            
        self.dnssrv = dnssrv
        self._onts = None
        self.parser = self._parser(ontology, ontpath)

        # Send as whom ?
        self.sender = sender
        # Send to whom ?
        self.receiver = receiver
        self.debug = debug
        self.verbose = verbose
        self._name = "om2api"
        if not log:
            self.log = [StdOutLogger({}, name="om2api")]
        else:
            self.log = log

    def _parser(self, ontologies=[], path=["."]):
        self._onts = twist.do_import(ontologies, path)
        return parse.RdfParse(self._onts)
        
    def absolute_class_type(self, shortname):
        for ontology in self._onts:
            try:
                objinst = ontology.ObjectFactory(shortname)
                return str(objinst.type)
            except KeyError:
                pass
        return None

    def absolute_property(self, shortname):
        for ontology in self._onts:
            try:
                prop = ontology.PropertyFactory(shortname)
                return str(prop.type)
            except KeyError:
                pass
        return None
        
    def absolute_property_by_object(self, shortname, typ=None):
        if not typ:
            return self.absolute_property(shortname)
        for ontology in self._onts:
            if self.verbose:
                log_info(self, "INSTANCE TYPE: %s" % typ)
            try:
                if self.verbose:
                    log_info(self, "%s" % ontology)
                obj = ontology.ObjectFactory(typ)
                if self.verbose:
                    log_info(self, "Found objecttype: %s" % obj)
                try:
                    return obj.class_property[shortname]
                except KeyError:
                    pass
                return None
            except KeyError:
                #if self.verbose:
                #    log_error(self, error_description(excep=e))
                pass
        return None

    def absolute_property_in_namespace(self, shortname="", namespace=None):
        if not namespace:
            return self.absolute_property(shortname)
        for ontology in self._onts:
            if str(ontology.NS) == namespace:
                prop = ontology.PropertyFactory(shortname)
                return str(prop.type)
        raise KeyError("Unkown property: %s" % shortname)
                
    def _class(self,classname):
        classname = str(classname)
        for ont in self._onts:
            try:
                return ont.ObjectFactory(classname)
            except KeyError:
                pass
        return None
    
    def _ontology(self, classname):
        classname = str(classname)
        for ont in self._onts:
            try:
                inst = ont.ObjectFactory(classname)
                return (ont, str(inst.type))
            except KeyError:
                pass
        return None
        
    def absolute(self, object_type, *arg):
        """
        When working with python classes built from OWL ontologies by
        rdfmarshal, the user will normally work with the short names
        that are property names of the class. But when for instance a
        SPARQL query are to be sent to the RDF repository then the full name
        of the properties has to be used. This function makes the translation
        from short to full names.
        
        :param object_type: The type/class of the object to which the 
            property belongs. Properties with the same name in different 
            classes may in fact be different properties and therefor have 
            different full name. Within one module (corresponds to one 
            ontology) property names are unique. So if only one ontology
            are involved no class specification are needed.
        :param arg: A set of short names to properties, this set can either
            be a simple string, a list or a dictionary where the key are
            the short name:
        :return: Short names converted to full names in a format that 
            correspons to the one used in arg.
        """
        if object_type:
            try:
                (ontology, atype) = self._ontology(object_type)
            except (AttributeError, TypeError):
                if self.debug:
                    log_debug(self,
                        "Could not find the type '%s' in my ontologies" % \
                            object_type)
                return None
            res = [fullname_from_ontology(ontology, a) for a in arg]
            return (atype, res)
        else:
            res = []
            for prop in arg:
                if isinstance(prop,basestring):
                    res.append(self.absolute_property(prop))
                elif isinstance(prop,list):
                    res.extend([self.absolute_property(prp) for prp in prop])
                elif isinstance(prop,dict):
                    res.append(dict([
                        (self.absolute_property(prp),v) \
                        for prp,v in prop.items()]))
            return res

    def _absolute_list(self, properties, object_type=None):
        """ Replace properties relative names with full names
        
        :param properties: A dictionary type object with property names
            as keys
        :param object_type: The type of object that has these properties
        :return: Tuple with object type and property/value dictionary
        """
        for ontology in self._onts:
            try:
                obj = ontology.ObjectFactory(object_type)
                obj_type = str(obj.type)
                proparr = {}
                for prop, varr in properties.items():
                    prop_type = str(ontology.PropertyFactory(prop).type)
                    proparr[prop_type] = varr
                return (obj_type, proparr)
            except KeyError:
                pass
        return None

    def _full(self, inst, key):
        if inst.allowed_property(key):
            if key in inst.class_property:
                return inst.class_property[key]
            else:
                return key
        else:
            if self.debug:
                log_debug(self, 
                    "Allowed properties:%s" % (inst.allowed_properties()))
            return None

    def _make_sparql_woo(self, filt={}, select=[], optional=[], 
                        about=True, regex=[]):
        if self.debug:
            log_debug(self, "Select:%s Optional:%s" % (select, optional))
        
        selset = set([])
        for key in select:
            fnkey = self.absolute_property(key)
            if fnkey:
                if fnkey not in filt.keys():
                    filt[fnkey] = None
                selset.add(fnkey)
            else:
                log_info(self, "Property '%s' not known" % key)

        optset = set([])            
        for key in optional:
            fnkey = self.absolute_property(key)
            if fnkey:
                optset.add(fnkey)
                selset.add(fnkey)
            else:
                log_info(self, "Property '%s' not known" % key)
                        
        regexp_verified = []
        for (prop,reg,arg) in regex:
            if prop in inst._known_as:
                regexp_verified.append((prop,reg,arg))
                
        
        if self.debug:
            log_debug(self, "filt: %s" %  filt)
            log_debug(self, "regex: %s" %  regexp_verified)
            log_debug(self, "select: %s" %  selset)
            log_debug(self, "optional: %s" %  optset)
        return sparql.query_from_dictionary(crit=filt, 
                select=list(selset), optd=list(optset), 
                regex=regexp_verified, about=about)

    def _make_sparql(self, object_type, filt={}, select=[], optional=[], 
                        about=True, regex=[]):
        if self.debug:
            log_debug(self, "Select:%s Optional:%s" % (select, optional))

        inst = self._class(object_type)
        if not inst:
            log_error(self, "Unknown object: %s" % (object_type))
            return None
        
        selset = set([])
        for key in select:
            fnkey = self._full(inst,key)
            if fnkey:
                if fnkey not in filt.keys():
                    filt[fnkey] = None
                selset.add(fnkey)
            else:
                log_info(self, "Property '%s' not found in '%s'" % \
                        (key, str(inst.type)))

        optset = set([])            
        for key in optional:
            fnkey = self._full(inst,key)
            if fnkey:
                optset.add(fnkey)
                selset.add(fnkey)
            else:
                log_info(self, "Property '%s' not found in '%s'" % \
                        (key, str(inst.type)))
                        
        regexp_verified = []
        for (prop,reg,arg) in regex:
            if prop in inst.reverse_name:
                regexp_verified.append((prop,reg,arg))
        
        if self.debug:
            log_debug(self, "object_type: %s" % inst.type)
            log_debug(self, "filt: %s" %  filt)
            log_debug(self, "regex: %s" %  regexp_verified)
            log_debug(self, "select: %s" %  selset)
            log_debug(self, "optional: %s" %  optset)
        return sparql.query_from_dictionary(str(inst.type), filt, 
                select=list(selset), optd=list(optset), 
                regex=regexp_verified, about=about)

    def _join(self, res):
        """
        :param res: 2-tuple containing (uriref,dic) or just uriref
        """
        clean_list = {}
        
        for item in res:
            #log_info(self, "ITEM: %s" % (item,))
            try:
                (uriref, ava) = item
            except ValueError:
                uriref = item
                ava = {}
                
            if not uriref: 
                try:
                    clean_list[""].append(ava)
                except KeyError:
                    clean_list[""] = [ava]
            if uriref in clean_list:
                #log_info(self, "TWO items with the same uriref: %s" % uriref)
                clean_list[uriref] = dict_join(clean_list[uriref],ava)
                #log_info(self, ">> combines to: %s" % (clean_list[uriref]),)
            else:
                clean_list[uriref] = ava
                
        result = []
        try:
            for val in clean_list[""]:
                result.append(("",val))
        except KeyError:
            pass
        
        for key, item in clean_list.items():
            if key == "":
                continue
            else:
                result.append((key,item))
                
        return result

    def search( self, object_type=None, filt={}, select=[], 
                sparql_query="", amap={}, optional=[], regex=[]):
        """
        Search for objects matching the criterias in the neorepo.
                
        :param object_type: The type of object to look for
        :param filt: A dictionary of properties and values to be used 
            to construct the SPARQL query. The FILTER !
        :param select: Which properties that should be returned, if "about",
            which is the default, only the urirefs will be returned.
        :param sparql: This is the alternative to building the sparql query from
            the above mentioned dictionary. Build your own query and supply it.
        :param optional: A set of properties that you'd like to have the 
            values for if there are any. 
        :param regex: When you expect this program to build the sparql query for
            you and you want to do approximate matching of some property, you have
            to say so by using this argument. regex is a list of 3-tuples containing
            property, regular expression and matching argument (see SPARQL doc).
        :return: List of URIRefs of the objects matching the query, or
            list of 2-tuples containing URIRef and property-values assertions
            as a dictionary. 
        """            
        if sparql_query:
            query = sparql_query            
        else:
            try:
                if object_type:
                    (query, amap) = self._make_sparql(object_type, filt, 
                                        select, optional, True, regex)
                else:
                    (query, amap) = self._make_sparql_woo(filt, 
                                        select, optional, True, regex)
            except Exception, excp:
                log_info(self, "exception: %s" % excp)
                log_error(self, error_description(excep=excp))
                return None
                
        if self.debug or self.verbose:
            log_debug(self, "QUERY: %s" % safe_str(query))
            log_debug(self, "Amap: %s" % amap)
        # result is a list of 2-tuples consisting of the object identifier
        #  and the object
        result = self.sparql.query(query, amap, self.debug)
        if self.debug:
            log_debug(self, "Result: %s" % result)
        if select == ["about"]:
            return list(set([uri for (uri, obj) in result]))
        else:
            return self._join(result)
        
    def read( self, uri):
        """
        Read an object from the neorepo.
        
        :param uri: The NeoRepo URI of the object that is wanted
        :return: The Object as a rdfmarshal Base class instance.
        """            
        lookup_uri = self.lookup_path+urllib.quote_plus(uri)
        if self.verbose:
            log_info(self, "lookup URL: %s" % lookup_uri)
        (response, content) = self.neorepo.request(lookup_uri)
        if self.debug:
            log_info(self, "Response: %s" % response)
            log_info(self, "Content: %s" % content)
        if response.status == 200:
            objlist = self.parser.parse(StringInputSource(content))
            if len(objlist) == 1:
                return objlist[0]
            else: # What ?
                raise ValueError
        elif response.status == 500:
            return None
        else:
            raise Exception("status:%s" % response.status)

    def apply( self, rdf, block_until_handled=True ):
        """
        Applies the operations in the RDF XML graph to the 'cloud',
        that is sends it to 'nexthop'.
        
        :param rdf: The RDF graph as a rdfmashal.base.Base instance
        :param block_until_handled: Whether the operations should be done 
            synchronous or asynchronous. This *only* works if the next hop
            node is of the type that will not ACK until the repository has
            applied the operation. 
        :return: True/False
        """
        rdf_xml = rdf.graph("pretty-xml")
        if self.debug:
            log_info(self,"RDF_XML: %s" % rdf_xml)
        if self.nexthop:
            try:
                return self.nexthop.write(rdf_xml)
            except socket.error:
                return False
        else:
            return rdf_xml

    def add(self, objekt, args={}):
        """
        Sends an add operation to the 'cloud' represented by 'nexthop'
        
        :param object: The object to be added to the neorepo.
        :param args: Some properties in the message object are set according
            to the configuration of self. These are 'sender' and 'receiver'.
            'body' if filled by object. 
            Other like 'createTime' and 'mid' are set by lower layer
            functions but can be change by using args. 
            The rest 'ErrorTo', 'ReplyTo' and so forth are not set unless
            done so by the use of this argument.
        :return: True/False
        """
        msg = miroop.new_add_message(objekt, self.sender, self.receiver, 
                    args=args)
        if self.verbose:
            log_info(self, "Add message: %s" % msg.dumps())
        if self.nexthop:
            return self.apply(msg)
        else:
            return msg

    def delete(self, uriref, args={}):
        """
        Sends a delete operation to the node 'nexthop'
        
        :param urief: The URIRef of the object
        :param args: Some properties in the message object are set according
            to the configuration of self. These are 'sender' and 'receiver'.
            'body' if filled by object. 
            Other like 'createTime' and 'mid' are set by lower layer
            functions but can be change by using args. 
            The rest 'ErrorTo', 'ReplyTo' and so forth are not set unless
            done so by the use of this argument.
        :return: True/False
        """
        msg = miroop.new_delete_message(uriref, self.sender, self.receiver, 
                    args=args)
        if self.nexthop:
            return self.apply(msg) 
        else:
            return msg

    def _do_string(self, val, obj, key):
        prop = obj.property(key)
        for ran in prop.range():
            if isinstance(ran, type):
                #print "TYPE type",ran
                if base.Base in ran.__mro__:
                    return URIRef(val)
        
        return set_lang(val)

    def _do_value(self,val, obj, key):
        if isinstance(val,dict):
            (object_type, uriref, ava) = unfurl(val)
            return self.make_object(object_type,uriref,ava)
        else:
            return self._do_string(val, obj, key)
            
    def make_object(self, objekt_type, uriref=None, ava={}):
        for ont in self._onts:
            try:
                obj = ont.ObjectFactory(objekt_type)
                if uriref != None:
                    obj.about = uriref
                if ava:
                    for key,values in ava.items():
                        if values == None:
                            new = None
                        elif isinstance(values, basestring):
                            new = self._do_string(values, obj, key)
                        elif isinstance(values,dict):
                            new = self._do_value(values, obj, key)
                        elif isinstance(values,list):
                            prop = obj.property(key)
                            new = [self._do_value(val, obj, key) for val in values]
                        else:
                            new = values
                        #print new,type(new)
                        obj[key] = new
                return obj
            except KeyError:
                pass
        raise Exception("Couldn't create object of type: '%s' (%s)" % \
            (objekt_type, self._onts))
        
    def insert( self, obj, create=False, args={}):
        """
        Sends a insert operation to 'nexthop' that will
        modify the the object in questions to include the given information
        
        :param obj: The object containing the changes
        :param create: If the object does not exist, try to create it
        :param args: Some properties in the message object are set according
            to the configuration of self. These are 'sender' and 'receiver'.
            'body' if filled by object. 
            Other like 'createTime' and 'mid' are set by lower layer
            functions but can be change by using args. 
            The rest 'ErrorTo', 'ReplyTo' and so forth are not set unless
            done so by the use of this argument.
        :return: True/False
        """
        
        match = self.make_object(str(obj.type),obj.about)
        if create:
            obj.about = ""
            msg = miroop.new_insert_or_add_message(match, obj, obj, 
                            self.sender, self.receiver, args)
        else:
            msg = miroop.new_insert_message(obj, self.sender, self.receiver, 
                                            match=match, args=args)
        if self.debug:
            log_debug(self,"insert: %s" % msg.struct())
        if self.nexthop:
            return self.apply(msg)
        else:
            return msg

    def remove(self, object, args={}):
        """Removes a part of an object. A remove operation is always a M 
        operation in MIRO parlance.
        
        :param object: The objectpart that should be removed
        :param args: Some properties in the message object are set according
            to the configuration of self. These are 'sender' and 'receiver'.
            'body' if filled by object. 
            Other like 'createTime' and 'mid' are set by lower layer
            functions but can be change by using args. 
            The rest 'ErrorTo', 'ReplyTo' and so forth are not set unless
            done so by the use of this argument.
        :return: True/False
        """

        body = miroop.new_body({"M":object})
        msg = miroop.new_message(body, self.sender, self.receiver, args=args)

        if self.debug:
            log_debug(self,"Remove: %s" % msg.struct())
        if self.nexthop:
            return self.apply(msg)
        else:
            return msg
            
    def replace(self, match, replace, args={}):
        """An replace information operation is always a MR operation in MIRO 
        parlance.
        
        :param match: The part of the object I want to replace
        :param replace: The part it should be replaced with
        :param args: Some properties in the message object are set according
            to the configuration of self. These are 'sender' and 'receiver'.
            'body' if filled by object. 
            Other like 'createTime' and 'mid' are set by lower layer
            functions but can be change by using args. 
            The rest 'ErrorTo', 'ReplyTo' and so forth are not set unless
            done so by the use of this argument.
        :return: True/False
        """
        # make sure the don't both have the same uriref's
        # match has to have a uriref
        
        if not match.about:
            raise Exception("Match clause without uriref not allowed")
        
        if replace.about:
            if replace.about != match.about:
                raise Exception("Mismatched urirefs not allowed")
            replace.about = None
        
        body = miroop.new_body({"M":match, "R":replace})
        msg = miroop.new_message(body, self.sender, self.receiver, args=args)
        if self.debug:
            log_debug(self,"Replace: %s" % msg.struct())
        if self.nexthop:
            ret = self.apply(msg)
            if self.debug:
                log_debug(self,"Replace returned: %s" % ret)
            return ret
        else:
            return msg

    def insert_replace(self, match, insert, replace, args={}):
        """An replace information operation is always a MR operation in MIRO 
        parlance.
        
        :param match: The part of the object I want to replace
        :param insert: The part that should be inserted
        :param replace: The part it should be replaced with
        :param args: Some properties in the message object are set according
            to the configuration of self. These are 'sender' and 'receiver'.
            'body' if filled by object. 
            Other like 'createTime' and 'mid' are set by lower layer
            functions but can be change by using args. 
            The rest 'ErrorTo', 'ReplyTo' and so forth are not set unless
            done so by the use of this argument.
        :return: True/False
        """
        # make sure the don't both have the same uriref's
        # match has to have a uriref
        
        if not match.about:
            raise Exception("Match clause without uriref not allowed")
        
        if insert.about:
            if insert.about != match.about:
                raise Exception("Mismatched urirefs not allowed")
            insert.about = None
            if insert.type != match.type:
                raise Exception("Types on match and insert differ")
        if replace.about:
            if replace.about != match.about:
                raise Exception("Mismatched urirefs not allowed")
            replace.about = None
        
        body = miroop.new_body({"M":match, "R":replace, "I":insert})
        msg = miroop.new_message(body, self.sender, self.receiver, args=args)
        if self.debug:
            log_debug(self,"Insert replace: %s" % msg.struct())
        if self.nexthop:
            return self.apply(msg)
        else:
            return msg

