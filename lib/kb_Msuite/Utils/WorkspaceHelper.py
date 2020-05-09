from kb_Msuite.Utils.Utils import Base, LogMixin


class WorkspaceHelper(Base, LogMixin):

    def __init__(self, checkMUtil_obj):
        self.checkMUtil = checkMUtil_obj
        self.client_util = checkMUtil_obj.client_util

    def client(self, client_name, *args):

        return self.client_util.client(client_name, *args)

    def _run_workspace_command(self, command, *args):

        ''' Thin wrapper with error handling around performing a workspace command '''

        try:
            # get the workspace method and call it with the provided args
            method = getattr(self.client('Workspace'), command)
            result = method(args)
        except Exception as e:
            self.logger.error({
                'command': command,
                'args': args,
                'error': e
            })
            err_str = 'Unable to perform workspace command "' + command + '": ' + str(e)
            raise ValueError(err_str)
            # to get the full stack trace: traceback.format_exc()
        return result

    def get_objects_from_workspace(self, object_ref):
        result = self._run_workspace_command('get_objects2', {
            'objects': [{'ref': object_ref}]
        })
        return result

    def get_obj_from_workspace(self, object_ref):
        return self.get_objects_from_workspace(object_ref)['data'][0]['data']

    def get_ws_obj_info(self, ref):
        result = self._run_workspace_command('get_object_info3', {
            'objects': [{'ref': ref}]
        })
        return result['infos'][0]

    def get_object_property(self, object_info, prop):
        '''
            given object_info, the array of object information fetched using the workspace
            get_object_info3 command, return the property 'prop'
        '''

        obj_property = {
            'id': 0,            # 0 obj_id objid - the numerical id of the object
            'name': 1,          # 1 obj_name name - the name of the object
            'type': 2,          # 2 type_string type - the type of the object
            'timestamp': 3,     # 3 timestamp save_date - the save date of the object
            'version': 4,       # 4 obj_ver ver - the version of the object
            'username': 5,      # 5 username saved_by - the user that saved or copied the object
            'ws_id': 6,         # 6 ws_id wsid - the workspace containing the object
            'ws_name': 7,       # 7 ws_name workspace - the workspace containing the object
            'chsum': 8,         # 8 the md5 checksum of the object
            'size': 9,          # 9 the size of the object in bytes
            'usermeta': 10      # 10 arbitrary user-supplied metadata about the object
        }

        if prop not in obj_property:
            raise KeyError(prop + ' is not a valid workspace object property')

        return object_info[obj_property[prop]]

    def get_ws_obj_name(self, ref=None, object_info=None):

        if not object_info:
            if not ref:
                raise ValueError("Must supply either ref or object_info to get_ws_obj_name")
            object_info = self.get_ws_obj_info(ref)

        return self.get_object_property(object_info, 'name')

    def get_ws_obj_type(self, ref=None, object_info=None, remove_module=False):

        if not object_info:
            if not ref:
                raise ValueError("Must supply either ref or object_info to get_ws_obj_type")
            object_info = self.get_ws_obj_info(ref)

        raw_type = self.get_object_property(object_info, 'type')
        obj_type = raw_type.split('-')[0]
        if remove_module:
            obj_type = obj_type.split('.')[1]
        return obj_type
