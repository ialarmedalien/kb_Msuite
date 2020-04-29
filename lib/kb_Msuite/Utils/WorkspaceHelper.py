from kb_Msuite.Utils.Logger import Base, LogMixin

class WorkspaceHelper(Base, LogMixin):

    def __init__(self, checkMUtil_obj):
        self.checkMUtil = checkMUtil_obj
        self.client_util = checkMUtil_obj.client_util

    def client(self, client_name):

        return self.client_util.client(client_name)

    def run_config(self):

        return self.checkMUtil.run_config()

    def get_workspace_object_info(self, ref):
        return self.client('Workspace').get_object_info3({'objects': [{'ref': ref}]})['infos'][0]

    def get_object_property(self, object_info, prop):
        '''
            given object_info, the array of object information fetched using the workspace
            get_object_info3 command, return the property 'prop'
        '''

        obj_property = {
            'id': 0,        # 0 obj_id objid - the numerical id of the object
            'name': 1,      # 1 obj_name name - the name of the object
            'type': 2,      # 2 type_string type - the type of the object
            'timestamp': 3, # 3 timestamp save_date - the save date of the object
            'version': 4,   # 4 obj_ver ver - the version of the object
            'username': 5,  # 5 username saved_by - the user that saved or copied the object
            'ws_id': 6,     # 6 ws_id wsid - the workspace containing the object
            'ws_name': 7,   # 7 ws_name workspace - the workspace containing the object
            'chsum': 8,     # 8 string chsum - the md5 checksum of the object
            'size': 9,      # 9 int size - the size of the object in bytes
            'usermeta': 10  # 10 usermeta meta - arbitrary user-supplied metadata about the object
        }

        if prop not in obj_property:
            raise KeyError(prop + ' is not a valid workspace object property')

        return object_info[obj_property[prop]]

    def get_data_obj_type_by_name(self, ref, remove_module=False):

        object_info = self.get_workspace_object_info(ref)
        obj_name = self.get_object_property(object_info, 'name')
        raw_type = self.get_object_property(object_info, 'type')
        obj_type = raw_type.split('-')[0]
        if remove_module:
            obj_type = obj_type.split('.')[1]
        return {obj_name: obj_type}

    def get_data_obj_name(self, ref):

        object_info = self.get_workspace_object_info(ref)
        return self.get_object_property(object_info, 'name')

    def get_data_obj_type(self, ref, remove_module=False):

        object_info = self.get_workspace_object_info(ref)
        raw_type = self.get_object_property(object_info, 'type')
        obj_type = raw_type.split('-')[0]
        if remove_module:
            obj_type = obj_type.split('.')[1]
        return obj_type

    def get_obj_from_workspace(self, object_ref):

        try:
            workspace_object = self.client('Workspace').get_objects2({'objects': [{'ref': object_ref}]})['data'][0]['data']
        except Exception as e:
            err_str = 'Unable to fetch ' + str(object_ref) + ' object from workspace: ' + str(e)
            raise ValueError(err_str)
            # to get the full stack trace: traceback.format_exc()
        return workspace_object
