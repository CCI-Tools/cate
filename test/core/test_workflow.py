import json
import os.path
from collections import OrderedDict
from unittest import TestCase

from cate.core.op import op_input, op_output, Operation
from cate.core.workflow import OpStep, Workflow, WorkflowStep, NodePort, ExpressionStep, NoOpStep, SubProcessStep, ValueCache, \
    SourceRef, new_workflow_op
from cate.util import UNDEFINED
from cate.util.misc import object_to_qualified_name
from cate.util.opmetainf import OpMetaInfo


@op_input('x')
@op_output('y')
def op1(x):
    return {'y': x + 1}


@op_input('a')
@op_output('b')
def op2(a):
    return {'b': 2 * a}


@op_input('u')
@op_input('v')
@op_output('w')
def op3(u, v, c=0):
    return {'w': 2 * u + 3 * v + c}


def get_resource(rel_path):
    return os.path.join(os.path.dirname(__file__), rel_path).replace('\\', '/')


class WorkflowTest(TestCase):
    @classmethod
    def create_example_3_steps_workflow(cls):
        step1 = OpStep(op1, node_id='op1')
        step2 = OpStep(op2, node_id='op2')
        step3 = OpStep(op3, node_id='op3')
        workflow = Workflow(OpMetaInfo('myWorkflow', inputs=OrderedDict(p={}), outputs=OrderedDict(q={})))
        workflow.add_steps(step1, step2, step3)
        step1.inputs.x.source = workflow.inputs.p
        step2.inputs.a.source = step1.outputs.y
        step3.inputs.u.source = step1.outputs.y
        step3.inputs.v.source = step2.outputs.b
        workflow.outputs.q.source = step3.outputs.w
        return step1, step2, step3, workflow

    def test_sorted_steps(self):
        step1, step2, step3, workflow = self.create_example_3_steps_workflow()
        sorted_steps = workflow.sorted_steps
        self.assertEqual(sorted_steps, [step1, step2, step3])

    def test_sort_steps(self):
        step1, step2, step3, _ = self.create_example_3_steps_workflow()
        self.assertEqual(Workflow.sort_steps([]), [])
        self.assertEqual(Workflow.sort_steps([step1]), [step1])
        self.assertEqual(Workflow.sort_steps([step1, step2]), [step1, step2])
        self.assertEqual(Workflow.sort_steps([step1, step3]), [step1, step3])
        self.assertEqual(Workflow.sort_steps([step2, step1]), [step1, step2])
        self.assertEqual(Workflow.sort_steps([step3, step1]), [step1, step3])
        self.assertEqual(Workflow.sort_steps([step2, step3]), [step2, step3])
        self.assertEqual(Workflow.sort_steps([step3, step2]), [step2, step3])
        self.assertEqual(Workflow.sort_steps([step1, step2, step3]), [step1, step2, step3])
        self.assertEqual(Workflow.sort_steps([step2, step1, step3]), [step1, step2, step3])
        self.assertEqual(Workflow.sort_steps([step3, step2, step1]), [step1, step2, step3])
        self.assertEqual(Workflow.sort_steps([step1, step3, step2]), [step1, step2, step3])

    def test_find_steps_to_compute(self):
        step1, step2, step3, workflow = self.create_example_3_steps_workflow()
        self.assertEqual(workflow.find_steps_to_compute('op1'), [step1])
        self.assertEqual(workflow.find_steps_to_compute('op2'), [step1, step2])
        self.assertEqual(workflow.find_steps_to_compute('op3'), [step1, step2, step3])

    def test_requires(self):
        step1, step2, step3, workflow = self.create_example_3_steps_workflow()
        self.assertFalse(step1.requires(step2))
        self.assertFalse(step1.requires(step3))
        self.assertFalse(step2.requires(step3))
        self.assertTrue(step2.requires(step1))
        self.assertTrue(step3.requires(step2))
        self.assertTrue(step3.requires(step1))

    def test_max_distance_to(self):
        step1, step2, step3, workflow = self.create_example_3_steps_workflow()
        self.assertEqual(step1.max_distance_to(step2), -1)
        self.assertEqual(step1.max_distance_to(step3), -1)
        self.assertEqual(step2.max_distance_to(step3), -1)
        self.assertEqual(step2.max_distance_to(step1), 1)
        self.assertEqual(step3.max_distance_to(step2), 1)
        self.assertEqual(step3.max_distance_to(step1), 2)
        self.assertEqual(step3.max_distance_to(step3), 0)

    def test_add_step(self):
        step1, step2, step3, workflow = self.create_example_3_steps_workflow()
        self.assertEqual(workflow.steps, [step1, step2, step3])
        old_step = workflow.add_step(step2, can_exist=True)
        self.assertIs(old_step, step2)
        self.assertEqual(workflow.steps, [step1, step2, step3])
        with self.assertRaises(ValueError):
            workflow.add_step(step2, can_exist=False)

    def test_remove_step(self):
        step1, step2, step3, workflow = self.create_example_3_steps_workflow()
        self.assertEqual(workflow.steps, [step1, step2, step3])
        old_step = workflow.remove_step(step3, must_exist=False)
        self.assertIs(old_step, step3)
        self.assertEqual(workflow.steps, [step1, step2])
        old_step = workflow.remove_step(step3, must_exist=False)
        self.assertIs(old_step, None)
        self.assertEqual(workflow.steps, [step1, step2])
        with self.assertRaises(ValueError):
            workflow.remove_step(step3, must_exist=True)

    def test_init(self):
        step1, step2, step3, workflow = self.create_example_3_steps_workflow()

        self.assertEqual(workflow.id, 'myWorkflow')
        self.assertEqual(len(workflow.inputs), 1)
        self.assertEqual(len(workflow.outputs), 1)
        self.assertIn('p', workflow.inputs)
        self.assertIn('q', workflow.outputs)

        self.assertEqual(workflow.steps, [step1, step2, step3])

        self.assertIsNone(workflow.inputs.p.source)
        self.assertIsNone(workflow.inputs.p.value)

        self.assertIs(workflow.outputs.q.source, step3.outputs.w)
        self.assertIsNone(workflow.outputs.q.value)

        self.assertEqual(str(workflow), workflow.id + ' = myWorkflow(p=None) -> (q=@op3.w) [Workflow]')
        self.assertEqual(repr(workflow), "Workflow('myWorkflow')")

    def test_invoke(self):
        _, _, _, workflow = self.create_example_3_steps_workflow()

        workflow.inputs.p.value = 3
        workflow.invoke()
        output_value = workflow.outputs.q.value
        self.assertEqual(output_value, 2 * (3 + 1) + 3 * (2 * (3 + 1)))

    def test_invoke_with_cache(self):
        _, _, _, workflow = self.create_example_3_steps_workflow()

        value_cache = dict()
        workflow.inputs.p.value = 3
        workflow.invoke(context=dict(value_cache=value_cache))
        output_value = workflow.outputs.q.value
        self.assertEqual(output_value, 2 * (3 + 1) + 3 * (2 * (3 + 1)))
        self.assertEqual(value_cache, dict(op1={'y': 4}, op2={'b': 8}, op3={'w': 32}))

    def test_invoke_with_context_inputs(self):
        def some_op(context, workflow, workflow_id, step, step_id, invalid):
            return dict(context=context,
                        workflow=workflow,
                        workflow_id=workflow_id,
                        step=step,
                        step_id=step_id,
                        invalid=invalid)

        from cate.core.op import OP_REGISTRY

        try:
            op_reg = OP_REGISTRY.add_op(some_op)
            op_reg.op_meta_info.inputs['context']['context'] = True
            op_reg.op_meta_info.inputs['workflow']['context'] = 'workflow'
            op_reg.op_meta_info.inputs['workflow_id']['context'] = 'workflow.id'
            op_reg.op_meta_info.inputs['step']['context'] = 'step'
            op_reg.op_meta_info.inputs['step_id']['context'] = 'step.id'
            op_reg.op_meta_info.inputs['invalid']['context'] = 'gnarz[8]'

            step = OpStep(op_reg, node_id='test_step')

            workflow = Workflow(OpMetaInfo('test_workflow'))
            workflow.add_step(step)
            workflow.invoke()

            output = step.outputs['return'].value
            self.assertIsInstance(output, dict)
            self.assertIsInstance(output.get('context'), dict)
            self.assertIs(output.get('workflow'), workflow)
            self.assertEqual(output.get('workflow_id'), 'test_workflow')
            self.assertIs(output.get('step'), step)
            self.assertEqual(output.get('step_id'), 'test_step')
            self.assertEqual(output.get('invalid', 1), None)

        finally:
            OP_REGISTRY.remove_op(some_op)

    def test_call(self):
        _, _, _, workflow = self.create_example_3_steps_workflow()

        output_value_1 = workflow(p=3)
        self.assertEqual(output_value_1, dict(q=2 * (3 + 1) + 3 * (2 * (3 + 1))))

        output_value_2 = workflow.call(input_values=dict(p=3))
        self.assertEqual(output_value_1, output_value_2)

    def test_new_workflow_op(self):
        _, _, _, workflow = self.create_example_3_steps_workflow()

        op = new_workflow_op(workflow)
        self.assertEqual(op.op_meta_info.qualified_name, 'myWorkflow')
        self.assertEqual(op(p=3), dict(q=2 * (3 + 1) + 3 * (2 * (3 + 1))))

    def test_from_json_dict(self):
        workflow_json_text = """
        {
            "qualified_name": "my_workflow",
            "header": {
                "description": "My workflow is not too bad."
            },
            "inputs": {
                "p": {"description": "Input 'p'"}
            },
            "outputs": {
                "q": {"source": "op3.w", "description": "Output 'q'"}
            },
            "steps": [
                {
                    "id": "op1",
                    "op": "test.core.test_workflow.op1",
                    "inputs": {
                        "x": { "source": ".p" }
                    }
                },
                {
                    "id": "op2",
                    "op": "test.core.test_workflow.op2",
                    "inputs": {
                        "a": {"source": "op1"}
                    }
                },
                {
                    "id": "op3",
                    "persistent": true,
                    "op": "test.core.test_workflow.op3",
                    "inputs": {
                        "u": {"source": "op1.y"},
                        "v": {"source": "op2.b"}
                    }
                }
            ]
        }
        """
        workflow_json_dict = json.loads(workflow_json_text)
        workflow = Workflow.from_json_dict(workflow_json_dict)

        self.assertIsNotNone(workflow)
        self.assertEqual(workflow.id, "my_workflow")

        self.assertEqual(workflow.op_meta_info.qualified_name, workflow.id)
        self.assertEqual(workflow.op_meta_info.header, dict(description="My workflow is not too bad."))
        self.assertEqual(len(workflow.op_meta_info.inputs), 1)
        self.assertEqual(len(workflow.op_meta_info.outputs), 1)
        self.assertEqual(workflow.op_meta_info.inputs['p'], dict(description="Input 'p'"))
        self.assertEqual(workflow.op_meta_info.outputs['q'], dict(source="op3.w", description="Output 'q'"))

        self.assertEqual(len(workflow.inputs), 1)
        self.assertEqual(len(workflow.outputs), 1)

        self.assertIn('p', workflow.inputs)
        self.assertIn('q', workflow.outputs)

        self.assertEqual(len(workflow.steps), 3)
        step1 = workflow.steps[0]
        step2 = workflow.steps[1]
        step3 = workflow.steps[2]

        self.assertEqual(step1.id, 'op1')
        self.assertEqual(step2.id, 'op2')
        self.assertEqual(step3.id, 'op3')

        self.assertIs(step1.inputs.x.source, workflow.inputs.p)
        self.assertIs(step2.inputs.a.source, step1.outputs.y)
        self.assertIs(step3.inputs.u.source, step1.outputs.y)
        self.assertIs(step3.inputs.v.source, step2.outputs.b)
        self.assertIs(workflow.outputs.q.source, step3.outputs.w)

        self.assertEqual(step1.persistent, False)
        self.assertEqual(step2.persistent, False)
        self.assertEqual(step3.persistent, True)

    def test_from_json_dict_empty(self):
        json_dict = json.loads('{"qualified_name": "hello"}')
        workflow = Workflow.from_json_dict(json_dict)
        self.assertEqual(workflow.id, 'hello')

    def test_from_json_dict_invalids(self):
        json_dict = json.loads('{"header": {}}')
        with self.assertRaises(ValueError) as cm:
            Workflow.from_json_dict(json_dict)
        self.assertEqual(str(cm.exception), 'missing mandatory property "qualified_name" in Workflow-JSON')

    def test_to_json_dict(self):
        step1 = OpStep(op1, node_id='op1')
        step2 = OpStep(op2, node_id='op2')
        step3 = OpStep(op3, node_id='op3')
        workflow = Workflow(OpMetaInfo('my_workflow', inputs=OrderedDict(p={}), outputs=OrderedDict(q={})))
        workflow.add_steps(step1, step2, step3)
        step1.inputs.x.source = workflow.inputs.p
        step2.inputs.a.source = step1.outputs.y
        step3.inputs.u.source = step1.outputs.y
        step3.inputs.v.source = step2.outputs.b
        workflow.outputs.q.source = step3.outputs.w

        step2.persistent = True

        workflow_dict = workflow.to_json_dict()

        expected_json_text = """
        {
            "schema_version": 1,
            "qualified_name": "my_workflow",
            "header": {},
            "inputs": {
                "p": {}
            },
            "outputs": {
                "q": {
                    "source": "op3.w"
                }
            },
            "steps": [
                {
                    "id": "op1",
                    "op": "test.core.test_workflow.op1",
                    "inputs": {
                        "x": "my_workflow.p"
                    }
                },
                {
                    "id": "op2",
                    "persistent": true,
                    "op": "test.core.test_workflow.op2",
                    "inputs": {
                        "a": "op1.y"
                    }
                },
                {
                    "id": "op3",
                    "op": "test.core.test_workflow.op3",
                    "inputs": {
                        "v": "op2.b",
                        "u": "op1.y"
                    }
                }
            ]
        }
        """

        actual_json_text = json.dumps(workflow_dict, indent=4)
        expected_json_obj = json.loads(expected_json_text)
        actual_json_obj = json.loads(actual_json_text)

        self.assertEqual(actual_json_obj, expected_json_obj,
                         msg='\nexpected:\n%s\n%s\nbut got:\n%s\n%s\n' %
                             (120 * '-', expected_json_text,
                              120 * '-', actual_json_text))


class ExpressionStepTest(TestCase):
    expression = "dict(x = 1 + 2 * a, y = 3 * b ** 2 + 4 * c ** 3)"

    def test_init(self):
        node = ExpressionStep(self.expression,
                              OrderedDict([('a', {}), ('b', {}), ('c', {})]),
                              OrderedDict([('x', {}), ('y', {})]),
                              node_id='bibo_8')
        self.assertEqual(node.id, 'bibo_8')
        self.assertEqual(str(node),
                         node.id + ' = "dict(x = 1 + 2 * a, y = 3 * b ** 2 + 4 * c ** 3)"(a=None, b=None, c=None) '
                                   '-> (x, y) [ExpressionStep]')
        self.assertEqual(repr(node), "ExpressionStep('%s', node_id='bibo_8')" % self.expression)

        node = ExpressionStep(self.expression)
        self.assertEqual(node.op_meta_info.inputs, {})
        self.assertEqual(node.op_meta_info.outputs, {'return': {}})

    def test_from_json_dict(self):
        json_text = """
        {
            "id": "bibo_8",
            "inputs": {
                "a": {},
                "b": {},
                "c": {}
            },
            "outputs": {
                "x": {},
                "y": {}
            },
            "expression": "%s"
        }
        """ % self.expression

        json_dict = json.loads(json_text)

        node = ExpressionStep.from_json_dict(json_dict)

        self.assertIsInstance(node, ExpressionStep)
        self.assertEqual(node.id, "bibo_8")
        self.assertIn('a', node.inputs)
        self.assertIn('b', node.inputs)
        self.assertIn('c', node.inputs)
        self.assertIn('x', node.outputs)
        self.assertIn('y', node.outputs)

    def test_to_json_dict(self):
        expected_json_text = """
        {
            "id": "bibo_8",
            "inputs": {
                "a": {},
                "b": {},
                "c": {}
            },
            "outputs": {
                "x": {},
                "y": {}
            },
            "expression": "%s"
        }
        """ % self.expression

        step = ExpressionStep(self.expression, inputs=OrderedDict(a={}, b={}, c={}), outputs=OrderedDict(x={}, y={}), node_id='bibo_8')
        actual_json_dict = step.to_json_dict()

        actual_json_text = json.dumps(actual_json_dict, indent=4)
        expected_json_dict = json.loads(expected_json_text)
        actual_json_dict = json.loads(actual_json_text)

        self.assertEqual(actual_json_dict, expected_json_dict,
                         msg='\n%sexpected:\n%s\n%s\nbut got:\n%s\n' %
                             (120 * '-', expected_json_text,
                              120 * '-', actual_json_text))

    def test_invoke(self):
        step = ExpressionStep(self.expression, OrderedDict(a={}, b={}, c={}), OrderedDict(x={}, y={}), node_id='bibo_8')
        a = 1.5
        b = -2.6
        c = 4.3
        step.inputs.a.value = a
        step.inputs.b.value = b
        step.inputs.c.value = c
        step.invoke()
        output_value_x = step.outputs.x.value
        output_value_y = step.outputs.y.value
        self.assertEqual(output_value_x, 1 + 2 * a)
        self.assertEqual(output_value_y, 3 * b ** 2 + 4 * c ** 3)

    def test_invoke_from_workflow(self):
        resource = get_resource('workflows/one_expr.json')
        workflow = Workflow.load(resource)
        a = 1.5
        b = -2.6
        c = 4.3
        workflow.inputs.a.value = a
        workflow.inputs.b.value = b
        workflow.inputs.c.value = c
        workflow.invoke()
        output_value_x = workflow.outputs.x.value
        output_value_y = workflow.outputs.y.value
        self.assertEqual(output_value_x, 1 + 2 * a)
        self.assertEqual(output_value_y, 3 * b ** 2 + 4 * c ** 3)


class WorkflowStepTest(TestCase):
    def test_init(self):
        resource = get_resource('workflows/three_ops.json')
        workflow = Workflow.load(resource)
        step = WorkflowStep(workflow, resource, node_id='jojo_87')
        self.assertEqual(step.id, 'jojo_87')
        self.assertEqual(step.resource, resource)
        self.assertEqual(str(step), 'jojo_87 = cool_workflow(p=None) -> (q) [WorkflowStep]')
        self.assertEqual(repr(step), "WorkflowStep(Workflow('cool_workflow'), '%s', node_id='jojo_87')" % resource)

        self.assertIsNotNone(step.workflow)
        self.assertIn('p', step.workflow.inputs)
        self.assertIn('q', step.workflow.outputs)

    def test_from_json_dict(self):
        resource = get_resource('workflows/three_ops.json')
        json_text = """
        {
            "id": "workflow_ref_89",
            "workflow": "%s",
            "inputs": {
                "p": {"value": 2.8}
            }
        }
        """ % resource

        json_dict = json.loads(json_text)

        step = WorkflowStep.from_json_dict(json_dict)

        self.assertIsInstance(step, WorkflowStep)
        self.assertEqual(step.id, "workflow_ref_89")
        self.assertEqual(step.resource, resource)
        self.assertIn('p', step.inputs)
        self.assertIn('q', step.outputs)
        self.assertEqual(step.inputs.p.value, 2.8)
        self.assertEqual(step.outputs.q.value, None)

        self.assertIsNotNone(step.workflow)
        self.assertIn('p', step.workflow.inputs)
        self.assertIn('q', step.workflow.outputs)

        self.assertIs(step.workflow.inputs.p.source, step.inputs.p)

    def test_to_json_dict(self):
        resource = get_resource('workflows/three_ops.json')
        workflow = Workflow.load(resource)
        step = WorkflowStep(workflow, resource, node_id='jojo_87')
        actual_json_dict = step.to_json_dict()

        expected_json_text = """
        {
            "id": "jojo_87",
            "workflow": "%s",
            "inputs": {
                "p": {}
            },
            "outputs": {
                "q": {}
            }
        }
        """ % resource

        actual_json_text = json.dumps(actual_json_dict, indent=4)
        expected_json_dict = json.loads(expected_json_text)
        actual_json_dict = json.loads(actual_json_text)

        self.assertEqual(actual_json_dict, expected_json_dict,
                         msg='\n%sexpected:\n%s\n%s\nbut got:\n%s\n' %
                             (120 * '-', expected_json_text,
                              120 * '-', actual_json_text))

    def test_invoke(self):
        resource = get_resource('workflows/three_ops.json')
        workflow = Workflow.load(resource)
        step = WorkflowStep(workflow, resource, node_id='jojo_87')

        value_cache = {}
        step.inputs.p.value = 3
        step.invoke(context=dict(value_cache=value_cache))
        output_value = step.outputs.q.value
        self.assertEqual(output_value, 2 * (3 + 1) + 3 * (2 * (3 + 1)))
        self.assertEqual(value_cache, {'op1': {'y': 4}, 'op2': {'b': 8}, 'op3': {'w': 32}})

    def test_invoke_as_part_of_workflow(self):
        resource = get_resource('workflows/three_ops.json')
        workflow = Workflow.load(resource)
        step = WorkflowStep(workflow, resource, node_id='jojo_87')

        workflow = Workflow(OpMetaInfo('contains_jojo_87',
                                       has_monitor=True,
                                       inputs=OrderedDict(x={}),
                                       outputs=OrderedDict(y={})))
        workflow.add_step(step)
        step.inputs.p.source = workflow.inputs.x
        workflow.outputs.y.source = step.outputs.q

        value_cache = ValueCache()
        workflow.inputs.x.value = 4
        workflow.invoke(context=dict(value_cache=value_cache))
        output_value = workflow.outputs.y.value
        self.assertEqual(output_value, 2 * (4 + 1) + 3 * (2 * (4 + 1)))
        self.assertEqual(value_cache, {'jojo_87._child': {'op1': {'y': 5}, 'op2': {'b': 10}, 'op3': {'w': 40}}})


class OpStepTest(TestCase):
    def test_init(self):
        step = OpStep(op3)

        self.assertRegex(step.id, '^opstep_[0-9a-f]+$')

        self.assertTrue(len(step.inputs), 2)
        self.assertTrue(len(step.outputs), 1)

        self.assertTrue(hasattr(step.inputs, 'u'))
        self.assertIs(step.inputs.u.node, step)
        self.assertEqual(step.inputs.u.name, 'u')

        self.assertTrue(hasattr(step.inputs, 'v'))
        self.assertIs(step.inputs.v.node, step)
        self.assertEqual(step.inputs.v.name, 'v')

        self.assertTrue(hasattr(step.outputs, 'w'))
        self.assertIs(step.outputs.w.node, step)
        self.assertEqual(step.outputs.w.name, 'w')

        self.assertEqual(str(step), step.id + ' = test.core.test_workflow.op3(u=None, v=None, c=0) -> (w) [OpStep]')
        self.assertEqual(repr(step), "OpStep('test.core.test_workflow.op3', node_id='%s')" % step.id)

    def test_init_operation_and_name_are_equivalent(self):
        step3 = OpStep(op3)
        self.assertIsNotNone(step3.op)
        self.assertIsNotNone(step3.op_meta_info)
        node31 = OpStep(object_to_qualified_name(op3))
        self.assertIs(node31.op, step3.op)
        self.assertIs(node31.op_meta_info, step3.op_meta_info)

    def test_invoke(self):
        step1 = OpStep(op1)
        step1.inputs.x.value = 3
        step1.invoke()
        output_value = step1.outputs.y.value
        self.assertEqual(output_value, 3 + 1)

        step2 = OpStep(op2)
        step2.inputs.a.value = 3
        step2.invoke()
        output_value = step2.outputs.b.value
        self.assertEqual(output_value, 2 * 3)

        step3 = OpStep(op3)
        step3.inputs.u.value = 4
        step3.inputs.v.value = 5
        step3.invoke()
        output_value = step3.outputs.w.value
        self.assertEqual(output_value, 2 * 4 + 3 * 5)

    def test_call(self):
        step1 = OpStep(op1)
        step1.inputs.x.value = 3
        output_value = step1(x=3)
        self.assertEqual(output_value, dict(y=3 + 1))

        step2 = OpStep(op2)
        output_value = step2(a=3)
        self.assertEqual(output_value, dict(b=2 * 3))

        step3 = OpStep(op3)
        output_value = step3(u=4, v=5)
        self.assertEqual(output_value, dict(w=2 * 4 + 3 * 5))

    def test_init_failures(self):
        with self.assertRaises(ValueError):
            # "ValueError: operation with name 'test_node.NodeTest' not registered"
            OpStep(OpStepTest)

        with self.assertRaises(ValueError):
            # "ValueError: operation with name 'X' not registered"
            OpStep('X')

        with self.assertRaises(ValueError):
            # "ValueError: operation with name 'X.Y' not registered"
            OpStep('X.Y')

        with self.assertRaises(ValueError):
            # "ValueError: operation must be given"
            OpStep(None)

    def test_connect_source(self):
        step1 = OpStep(op1)
        step2 = OpStep(op2)
        step3 = OpStep(op3)
        step2.inputs.a.source = step1.outputs.y
        step3.inputs.u.source = step1.outputs.y
        step3.inputs.v.source = step2.outputs.b
        self.assertConnectionsAreOk(step1, step2, step3)

        with self.assertRaises(AttributeError) as cm:
            step1.inputs.a.source = step3.inputs.u
        self.assertEqual(str(cm.exception), "attribute 'a' not found")

    def test_disconnect_source(self):
        step1 = OpStep(op1)
        step2 = OpStep(op2)
        step3 = OpStep(op3)

        step2.inputs.a.source = step1.outputs.y
        step3.inputs.u.source = step1.outputs.y
        step3.inputs.v.source = step2.outputs.b
        self.assertConnectionsAreOk(step1, step2, step3)

        step3.inputs.v.source = None

        self.assertIs(step2.inputs.a.source, step1.outputs.y)
        self.assertIs(step3.inputs.u.source, step1.outputs.y)

        step2.inputs.a.source = None

        self.assertIs(step3.inputs.u.source, step1.outputs.y)
        self.assertIs(step3.inputs.u.source, step1.outputs.y)

        step3.inputs.u.source = None

    def assertConnectionsAreOk(self, step1, step2, step3):
        self.assertIs(step2.inputs.a.source, step1.outputs.y)
        self.assertIs(step3.inputs.u.source, step1.outputs.y)
        self.assertIs(step3.inputs.v.source, step2.outputs.b)

    def test_from_json_dict_value(self):
        json_text = """
        {
            "id": "op3",
            "op": "test.core.test_workflow.op3",
            "inputs": {
                "u": {"value": 647},
                "v": {"value": 2.9}
            }
        }
        """

        json_dict = json.loads(json_text)

        step3 = OpStep.from_json_dict(json_dict)

        self.assertIsInstance(step3, OpStep)
        self.assertEqual(step3.id, "op3")
        self.assertIsInstance(step3.op, Operation)
        self.assertIn('u', step3.inputs)
        self.assertIn('v', step3.inputs)
        self.assertIn('w', step3.outputs)

        self.assertEqual(step3.inputs.u.value, 647)
        self.assertEqual(step3.inputs.v.value, 2.9)

    def test_from_json_dict_source(self):
        json_text = """
        {
            "id": "op3",
            "op": "test.core.test_workflow.op3",
            "inputs": {
                "u": {"source": "stat_op.stats"},
                "v": {"source": ".latitude"}
            }
        }
        """

        json_dict = json.loads(json_text)

        step3 = OpStep.from_json_dict(json_dict)

        self.assertIsInstance(step3, OpStep)
        self.assertEqual(step3.id, "op3")
        self.assertIsInstance(step3.op, Operation)
        self.assertIn('u', step3.inputs)
        self.assertIn('v', step3.inputs)
        self.assertIn('w', step3.outputs)
        self.assertEqual(step3.inputs.u._source_ref, ('stat_op', 'stats'))
        self.assertEqual(step3.inputs.u.source, None)
        self.assertEqual(step3.inputs.v._source_ref, (None, 'latitude'))
        self.assertEqual(step3.inputs.v.source, None)

    def test_to_json_dict(self):
        step3 = OpStep(op3, node_id='op3')
        step3.inputs.u.value = 2.8
        step3.inputs.c.value = 1

        step3_dict = step3.to_json_dict()

        expected_json_text = """
        {
            "id": "op3",
            "op": "test.core.test_workflow.op3",
            "inputs": {
                "u": {"value": 2.8},
                "c": {"value": 1}
            }
        }
        """

        actual_json_text = json.dumps(step3_dict)

        expected_json_obj = json.loads(expected_json_text)
        actual_json_obj = json.loads(actual_json_text)

        self.assertEqual(actual_json_obj, expected_json_obj,
                         msg='\n%sexpected:\n%s\n%s\nbut got:\n%s\n' %
                             (120 * '-', expected_json_text,
                              120 * '-', actual_json_text))

        # Invoke OpStep, and assert that output values are NOT serialized to JSON
        step3.inputs.u.value = 2.8
        step3.inputs.v.value = 1.2
        step3.invoke()
        step3_dict = step3.to_json_dict()

        expected_json_text = """
        {
            "id": "op3",
            "op": "test.core.test_workflow.op3",
            "inputs": {
                "v": {"value": 1.2},
                "u": {"value": 2.8},
                "c": {"value": 1}
            }
        }
        """

        actual_json_text = json.dumps(step3_dict)

        expected_json_obj = json.loads(expected_json_text)
        actual_json_obj = json.loads(actual_json_text)

        self.assertEqual(actual_json_obj, expected_json_obj,
                         msg='\n%sexpected:\n%s\n%s\nbut got:\n%s\n' %
                             (120 * '-', expected_json_text,
                              120 * '-', actual_json_text))


class NoOpStepTest(TestCase):
    def test_init(self):
        step = NoOpStep(inputs=OrderedDict([('a', {}), ('b', {})]),
                        outputs=OrderedDict([('c', {}), ('d', {})]))

        self.assertRegex(step.id, '^noopstep_[0-9a-f]+$')

        self.assertIsNotNone(step.op_meta_info)
        self.assertEqual(step.op_meta_info.qualified_name, step.id)

        self.assertTrue(len(step.inputs), 2)
        self.assertTrue(len(step.outputs), 2)

        self.assertTrue(hasattr(step.inputs, 'a'))
        self.assertIs(step.inputs.a.node, step)

        self.assertTrue(hasattr(step.outputs, 'd'))
        self.assertIs(step.outputs.d.node, step)

        self.assertEqual(str(step), step.id + ' = noop(a=None, b=None) -> (c, d) [NoOpStep]')
        self.assertEqual(repr(step), "NoOpStep(node_id='%s')" % step.id)

    def test_invoke(self):
        step = NoOpStep(inputs=OrderedDict([('a', {}), ('b', {})]),
                        outputs=OrderedDict([('c', {}), ('d', {})]))

        # Operation: Swap input
        step.outputs.c.source = step.inputs.b
        step.outputs.d.source = step.inputs.a

        step.inputs.a.value = 'A'
        step.inputs.b.value = 'B'
        step.invoke()
        self.assertEqual(step.outputs.c.value, 'B')
        self.assertEqual(step.outputs.d.value, 'A')

    def test_from_and_to_json(self):
        json_text = """
        {
            "id": "op3",
            "no_op": true,
            "inputs": {
                "a": {"value": 647},
                "b": {"value": 2.9}
            },
            "outputs": {
                "c": {"source": "op3.b"},
                "d": {"source": "op3.a"}
            }
        }
        """

        json_dict = json.loads(json_text)

        step = NoOpStep.from_json_dict(json_dict)

        self.assertIsInstance(step, NoOpStep)
        self.assertEqual(step.id, "op3")
        self.assertIn('a', step.inputs)
        self.assertIn('b', step.inputs)
        self.assertIn('c', step.outputs)
        self.assertIn('d', step.outputs)

        self.assertEqual(step.inputs.a.value, 647)
        self.assertEqual(step.inputs.b.value, 2.9)
        self.assertEqual(step.outputs.c._source_ref, ('op3', 'b'))
        self.assertEqual(step.outputs.d._source_ref, ('op3', 'a'))

        # json_dict_2 = step.to_json_dict()
        # self.assertEqual(json_dict, json_dict_2)


class SubProcessStepTest(TestCase):
    def test_init(self):
        step = SubProcessStep('cd {dir}',
                              inputs=OrderedDict(dir=dict(data_type=str)))

        self.assertRegex(step.id, '^subprocessstep_[0-9a-f]+$')

        self.assertIsNotNone(step.op_meta_info)
        self.assertEqual(step.op_meta_info.qualified_name, step.id)

        self.assertTrue(len(step.inputs), 1)
        self.assertTrue(len(step.outputs), 1)

        self.assertTrue(hasattr(step.inputs, 'dir'))
        self.assertIs(step.inputs.dir.node, step)

        self.assertTrue(hasattr(step.outputs, 'return'))
        self.assertIs(step.outputs['return'].node, step)

        self.assertEqual(str(step), step.id + ' = "cd {dir}"(dir=None) [SubProcessStep]')
        self.assertEqual(repr(step), "SubProcessStep('cd {dir}', node_id='%s')" % step.id)

    def test_invoke(self):
        step = SubProcessStep('cd {dir}',
                              shell=True,
                              inputs=OrderedDict([('dir', dict(data_type=str))]))

        step.inputs.dir.value = '..'

        step.invoke()
        self.assertEqual(step.outputs['return'].value, 0)

    def test_from_and_to_json(self):
        json_text = """
        {
            "id": "op3",
            "command": "cd {dir}",
            "cwd": ".",
            "env": {
                "JDK_HOME": "."
            },
            "shell": true,
            "inputs": {
                "dir": {"value": "."}
            }
        }
        """

        json_dict = json.loads(json_text)

        step = SubProcessStep.from_json_dict(json_dict)

        self.assertIsInstance(step, SubProcessStep)
        self.assertEqual(step.id, "op3")
        self.assertIn('dir', step.inputs)
        self.assertIn('return', step.outputs)
        self.assertEqual(step.inputs.dir.value, '.')

        expected_json_text = """
                {
                    "id": "op3",
                    "command": "cd {dir}",
                    "cwd": ".",
                    "env": {
                        "JDK_HOME": "."
                    },
                    "shell": true,
                    "inputs": {
                        "dir": {"value": "."}
                    },
                    "outputs": {
                        "return": {}
                    }
                }
                """
        expected_json_dict = json.loads(expected_json_text)
        actual_json_dict = step.to_json_dict()
        self.assertEqual(actual_json_dict, expected_json_dict)


class NodePortTest(TestCase):
    def test_init(self):
        step = OpStep(op1, node_id='myop')
        x_port = NodePort(step, 'x')

        self.assertIs(x_port.node, step)
        self.assertEqual(x_port.node_id, 'myop')
        self.assertEqual(x_port.name, 'x')
        self.assertEqual(x_port.is_source, False)
        self.assertEqual(x_port.source, None)
        self.assertEqual(x_port.source_ref, None)
        self.assertEqual(x_port.is_value, False)
        self.assertEqual(x_port.has_value, False)
        self.assertEqual(x_port.value, None)
        self.assertEqual(str(x_port), 'myop.x')
        self.assertEqual(repr(x_port), "NodePort('myop', 'x')")

    def test_source_and_value(self):
        step1 = OpStep(op1, node_id='op1')
        step2 = OpStep(op2, node_id='op2')

        x_port = NodePort(step1, 'x')
        b_port = NodePort(step2, 'b')

        self.assertEqual(x_port.is_source, False)
        self.assertEqual(x_port.source, None)
        self.assertEqual(x_port.source_ref, None)
        self.assertEqual(x_port.is_value, False)
        self.assertEqual(x_port.has_value, False)
        self.assertEqual(x_port.value, None)

        x_port.value = 11
        self.assertEqual(x_port.is_source, False)
        self.assertEqual(x_port.source, None)
        self.assertEqual(x_port.source_ref, None)
        self.assertEqual(x_port.is_value, True)
        self.assertEqual(x_port.has_value, True)
        self.assertEqual(x_port.value, 11)

        x_port.source = b_port
        self.assertEqual(x_port.is_source, True)
        self.assertEqual(x_port.source, b_port)
        self.assertEqual(x_port.source_ref, SourceRef(node_id='op2', port_name='b'))
        self.assertEqual(x_port.is_value, False)
        self.assertEqual(x_port.has_value, False)
        self.assertEqual(x_port.value, None)

        b_port.value = 67382
        self.assertEqual(x_port.is_source, True)
        self.assertEqual(x_port.source, b_port)
        self.assertEqual(x_port.source_ref, SourceRef(node_id='op2', port_name='b'))
        self.assertEqual(x_port.is_value, False)
        self.assertEqual(x_port.has_value, True)
        self.assertEqual(x_port.value, 67382)

        with self.assertRaises(ValueError) as cm:
            x_port.source = x_port
        self.assertEqual(str(cm.exception), "cannot connect 'op1.x' with itself")

        # TODO (forman): must check for cyclic dependency
        # with self.assertRaises(ValueError) as cm:
        #     x_port.source = y_port
        # self.assertEqual(str(cm.exception), "AAAAA")

    def test_resolve_source_ref(self):
        step1 = OpStep(op1, node_id='myop1')
        step2 = OpStep(op2, node_id='myop2')
        step2.inputs.a._source_ref = ('myop1', 'y')

        g = Workflow(OpMetaInfo('myWorkflow',
                                has_monitor=True,
                                inputs=OrderedDict(x={}),
                                outputs=OrderedDict(b={})))
        g.add_steps(step1, step2)

        step2.inputs.a.update_source()

        self.assertEqual(step2.inputs.a._source_ref, ('myop1', 'y'))
        self.assertIs(step2.inputs.a.source, step1.outputs.y)
        self.assertIs(step2.inputs.a.value, None)

    def test_from_json_dict(self):
        step2 = OpStep(op2, node_id='myop2')
        port2 = NodePort(step2, 'a')

        port2.from_json(json.loads('{"value": 2.6}'))
        self.assertEqual(port2._source_ref, None)
        self.assertEqual(port2._source, None)
        self.assertEqual(port2._value, 2.6)

        port2.from_json(json.loads('{"source": "myop1.y"}'))
        self.assertEqual(port2._source_ref, ('myop1', 'y'))
        self.assertEqual(port2._source, None)
        self.assertEqual(port2._value, UNDEFINED)

        # "myop1.y" is a shorthand for {"source": "myop1.y"}
        port2.from_json(json.loads('"myop1.y"'))
        self.assertEqual(port2._source_ref, ('myop1', 'y'))
        self.assertEqual(port2._source, None)
        self.assertEqual(port2._value, UNDEFINED)

        port2.from_json(json.loads('{"source": ".y"}'))
        self.assertEqual(port2._source_ref, (None, 'y'))
        self.assertEqual(port2._source, None)
        self.assertEqual(port2._value, UNDEFINED)

        # ".x" is a shorthand for {"source": ".x"}
        port2.from_json(json.loads('".y"'))
        self.assertEqual(port2._source_ref, (None, 'y'))
        self.assertEqual(port2._source, None)
        self.assertEqual(port2._value, UNDEFINED)

        # "myop1" is a shorthand for {"source": "myop1"}
        port2.from_json(json.loads('"myop1"'))
        self.assertEqual(port2._source_ref, ('myop1', None))
        self.assertEqual(port2._source, None)
        self.assertEqual(port2._value, UNDEFINED)

        # if "a" is defined, but neither "source" nor "value" is given, it will neither have a source nor a value
        port2.from_json(json.loads('{}'))
        self.assertEqual(port2._source_ref, None)
        self.assertEqual(port2._source, None)
        self.assertEqual(port2._value, UNDEFINED)
        port2.from_json(json.loads('null'))
        self.assertEqual(port2._source_ref, None)
        self.assertEqual(port2._source, None)
        self.assertEqual(port2._value, UNDEFINED)

        # if "a" is not defined at all, it will neither have a source nor a value
        port2.from_json(json.loads('{}'))
        self.assertEqual(port2._source_ref, None)
        self.assertEqual(port2._source, None)
        self.assertEqual(port2._value, UNDEFINED)

        with self.assertRaises(ValueError) as cm:
            port2.from_json(json.loads('{"value": 2.6, "source": "y"}'))
        self.assertEqual(str(cm.exception),
                         "error decoding 'myop2.a' because \"source\" and \"value\" are mutually exclusive")

        expected_msg = "error decoding 'myop2.a' because the \"source\" value format is " \
                       "neither \"<node-id>.<name>\", \"<node-id>\", nor \".<name>\""

        with self.assertRaises(ValueError) as cm:
            port2.from_json(json.loads('{"source": ""}'))
        self.assertEqual(str(cm.exception), expected_msg)

        with self.assertRaises(ValueError) as cm:
            port2.from_json(json.loads('{"source": "."}'))
        self.assertEqual(str(cm.exception), expected_msg)

        with self.assertRaises(ValueError) as cm:
            port2.from_json(json.loads('{"source": "var."}'))
        self.assertEqual(str(cm.exception), expected_msg)

    def test_to_json_dict(self):
        step1 = OpStep(op1, node_id='myop1')
        step2 = OpStep(op2, node_id='myop2')

        self.assertEqual(step2.inputs.a.to_json(), dict())

        step2.inputs.a.value = 982
        self.assertEqual(step2.inputs.a.to_json(), dict(value=982))
        self.assertEqual(step2.inputs.a.to_json(force_dict=True), dict(value=982))

        step2.inputs.a.source = step1.outputs.y
        self.assertEqual(step2.inputs.a.to_json(), 'myop1.y')
        self.assertEqual(step2.inputs.a.to_json(force_dict=True), dict(source='myop1.y'))

        step2.inputs.a.source = None
        self.assertEqual(step2.inputs.a.to_json(), dict())
        self.assertEqual(step2.inputs.a.to_json(force_dict=True), dict())


class ValueCacheTest(TestCase):
    class ClosableBibo:
        def __init__(self):
            self.closed = False

        def close(self):
            self.closed = True

    def test_close(self):
        bibo1 = ValueCacheTest.ClosableBibo()
        bibo2 = ValueCacheTest.ClosableBibo()
        bibo3 = ValueCacheTest.ClosableBibo()

        vc = ValueCache()
        vc['bibo1'] = bibo1
        vc['bibo2'] = bibo2
        vc['bibo3'] = bibo3

        self.assertFalse(bibo1.closed)
        self.assertFalse(bibo2.closed)
        self.assertFalse(bibo3.closed)
        vc.close()
        self.assertTrue(bibo1.closed)
        self.assertTrue(bibo2.closed)
        self.assertTrue(bibo3.closed)

    def test_close_with_child(self):
        bibo1 = ValueCacheTest.ClosableBibo()
        bibo2 = ValueCacheTest.ClosableBibo()
        bibo3 = ValueCacheTest.ClosableBibo()

        vc = ValueCache()
        vc['bibo1'] = bibo1
        vc['bibo2'] = bibo2
        bibo2_child = vc.child('bibo2')
        bibo2_child['bibo3'] = bibo3

        self.assertFalse(bibo1.closed)
        self.assertFalse(bibo2.closed)
        self.assertFalse(bibo3.closed)
        vc.close()
        self.assertTrue(bibo1.closed)
        self.assertTrue(bibo2.closed)
        self.assertTrue(bibo3.closed)

    def test_set(self):
        bibo = ValueCacheTest.ClosableBibo()

        vc = ValueCache()
        vc['bibo'] = bibo
        self.assertIn('bibo', vc)
        self.assertIs(vc['bibo'], bibo)

        self.assertFalse(bibo.closed)
        vc['bibo'] = None
        self.assertTrue(bibo.closed)
        self.assertIn('bibo', vc)
        self.assertIs(vc['bibo'], None)

    def test_del(self):
        bibo = ValueCacheTest.ClosableBibo()

        vc = ValueCache()
        vc['bibo'] = bibo
        self.assertIn('bibo', vc)
        self.assertIs(vc['bibo'], bibo)

        self.assertFalse(bibo.closed)
        del vc['bibo']
        self.assertTrue(bibo.closed)
        self.assertNotIn('bibo', vc)

    def test_child(self):
        bibo = object()

        vc = ValueCache()
        vc['bibo'] = bibo

        child_vc = vc.child('bibo')
        self.assertIsInstance(child_vc, ValueCache)
        self.assertIn('bibo', vc)
        self.assertIs(vc['bibo'], bibo)
        self.assertIn('bibo._child', vc)
        self.assertIs(vc['bibo._child'], child_vc)
        self.assertIsNot(child_vc, vc)

    def test_get_id(self):
        vc = ValueCache()
        vc['bibo1'] = object()
        vc['bibo2'] = object()
        vc['bibo3'] = object()

        self.assertEqual(vc.get_id('bibo1'), 1)
        self.assertEqual(vc.get_id('bibo2'), 2)
        self.assertEqual(vc.get_id('bibo3'), 3)

        vc['bibo1'] = object()
        vc['bibo2'] = object()
        vc['bibo3'] = object()

        self.assertEqual(vc.get_id('bibo1'), 1)
        self.assertEqual(vc.get_id('bibo2'), 2)
        self.assertEqual(vc.get_id('bibo3'), 3)

        vc.clear()

        self.assertEqual(vc.get_id('bibo1'), None)
        self.assertEqual(vc.get_id('bibo2'), None)
        self.assertEqual(vc.get_id('bibo3'), None)

        vc['bibo1'] = object()
        vc['bibo2'] = object()
        vc['bibo3'] = object()

        self.assertEqual(vc.get_id('bibo1'), 4)
        self.assertEqual(vc.get_id('bibo2'), 5)
        self.assertEqual(vc.get_id('bibo3'), 6)

    def test_get_update_count(self):
        vc = ValueCache()
        vc['bibo1'] = object()
        vc['bibo2'] = object()
        vc['bibo3'] = object()

        self.assertEqual(vc.get_update_count('bibo1'), 0)
        self.assertEqual(vc.get_update_count('bibo2'), 0)
        self.assertEqual(vc.get_update_count('bibo3'), 0)

        vc['bibo2'] = object()
        vc['bibo3'] = object()
        vc['bibo2'] = object()
        vc['bibo2'] = None

        self.assertEqual(vc.get_update_count('bibo1'), 0)
        self.assertEqual(vc.get_update_count('bibo2'), 3)
        self.assertEqual(vc.get_update_count('bibo3'), 1)

        vc.clear()

        self.assertEqual(vc.get_update_count('bibo1'), None)
        self.assertEqual(vc.get_update_count('bibo2'), None)
        self.assertEqual(vc.get_update_count('bibo3'), None)

    def test_rename_key(self):
        bibo = object()

        vc = ValueCache()
        vc['bibo'] = bibo

        bibo_id = vc.get_id('bibo')
        bibo_child = vc.child('bibo')

        vc.rename_key('bibo', 'bert')

        self.assertNotIn('bibo', vc)
        self.assertNotIn('bibo._child', vc)

        self.assertIn('bert', vc)
        self.assertIs(vc['bert'], bibo)
        self.assertIn('bert._child', vc)
        self.assertIs(vc['bert._child'], bibo_child)
        self.assertEqual(vc.get_id('bert'), bibo_id)
