from cate.core.monitor import Monitor
from cate.core.ds import DATA_STORE_REGISTRY
from cate.core.op import OP_REGISTRY


def type_to_str(data_type):
    if isinstance(data_type, str):
        return data_type
    elif hasattr(data_type, '__name__'):
        return data_type.__name__
    else:
        return str(data_type)


# noinspection PyMethodMayBeStatic
class Service:
    def get_data_stores(self):
        data_stores = DATA_STORE_REGISTRY.get_data_stores()
        data_store_list = []
        for data_store in data_stores:
            data_store_list.append(dict(id=data_store.name,
                                        name=data_store.name,
                                        description=''))
        return sorted(data_store_list, key=lambda item: item['name'])

    def get_data_sources(self, data_store_id: str, monitor: Monitor = Monitor.NONE):
        data_store = DATA_STORE_REGISTRY.get_data_store(data_store_id)
        if data_store is None:
            raise ValueError('Unknown data store: "%s"' % data_store_id)
        data_sources = data_store.query(monitor=monitor)
        data_source_list = []
        i = 0
        for data_source in data_sources:
            print(i, data_source.name)
            i += 1
            data_source_list.append(dict(id=data_source.name,
                                         name=data_source.name,
                                         meta_info=data_source.meta_info))
        return data_source_list

    def get_operations(self):
        op_list = []
        for op_name, op_reg in OP_REGISTRY.op_registrations.items():
            op_meta_info = op_reg.op_meta_info
            inputs = []
            for input_name, input_props in op_meta_info.input.items():
                inputs.append(dict(name=input_name,
                                   dataType=type_to_str(input_props.get('data_type', 'str')),
                                   description=input_props.get('description', '')))
            outputs = []
            for output_name, output_props in op_meta_info.output.items():
                outputs.append(dict(name=output_name,
                                    dataType=type_to_str(output_props.get('data_type', 'str')),
                                    description=output_props.get('description', '')))
            op_list.append(dict(
                name=op_name,
                tags=op_meta_info.header.get('tags', []),
                description=op_meta_info.header.get('description', ''),
                inputs=inputs,
                outputs=outputs))
        return op_list
