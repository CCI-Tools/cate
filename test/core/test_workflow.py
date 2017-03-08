import json
import os.path
from collections import OrderedDict
from unittest import TestCase

from cate.core.op import op_input, op_output, OpRegistration
from cate.util.opmetainf import OpMetaInfo
from cate.core.workflow import OpStep, Workflow, WorkflowStep, NodePort, ExprStep, NoOpStep, SubProcessStep
from cate.util.misc import object_to_qualified_name


@op_input('x')
@op_output('y')
class Op1:
    def __call__(self, x):
        return {'y': x + 1}


@op_input('a')
@op_output('b')
class Op2:
    def __call__(self, a):
        return {'b': 2 * a}


@op_input('u')
@op_input('v')
@op_output('w')
class Op3:
    def __call__(self, u, v):
        return {'w': 2 * u + 3 * v}


def get_resource(rel_path):
    return os.path.join(os.path.dirname(__file__), rel_path).replace('\\', '/')


class WorkflowTest(TestCase):
    @classmethod
    def create_example_3_steps_workflow(cls):
        step1 = OpStep(Op1, node_id='op1')
        step2 = OpStep(Op2, node_id='op2')
        step3 = OpStep(Op3, node_id='op3')
        workflow = Workflow(OpMetaInfo('myWorkflow', input_dict=OrderedDict(p={}), output_dict=OrderedDict(q={})))
        workflow.add_steps(step1, step2, step3)
        step1.input.x.source = workflow.input.p
        step2.input.a.source = step1.output.y
        step3.input.u.source = step1.output.y
        step3.input.v.source = step2.output.b
        workflow.output.q.source = step3.output.w
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
        self.assertEqual(len(workflow.input), 1)
        self.assertEqual(len(workflow.output), 1)
        self.assertIn('p', workflow.input)
        self.assertIn('q', workflow.output)

        self.assertEqual(workflow.steps, [step1, step2, step3])

        self.assertIsNone(workflow.input.p.source)
        self.assertIsNone(workflow.input.p.value)

        self.assertIs(workflow.output.q.source, step3.output.w)
        self.assertIsNone(workflow.output.q.value)

        self.assertEqual(str(workflow), workflow.id + ' = myWorkflow(p=None) -> (q=op3.w) [Workflow]')
        self.assertEqual(repr(workflow), "Workflow('myWorkflow')")

    def test_invoke(self):
        _, _, _, workflow = self.create_example_3_steps_workflow()

        workflow.input.p.value = 3
        workflow.invoke()
        output_value = workflow.output.q.value
        self.assertEqual(output_value, 2 * (3 + 1) + 3 * (2 * (3 + 1)))

    def test_invoke_with_cache(self):
        _, _, _, workflow = self.create_example_3_steps_workflow()

        value_cache = dict()
        workflow.input.p.value = 3
        workflow.invoke(value_cache=value_cache)
        output_value = workflow.output.q.value
        self.assertEqual(output_value, 2 * (3 + 1) + 3 * (2 * (3 + 1)))
        self.assertEqual(value_cache, dict(op1={'y': 4}, op2={'b': 8}, op3={'w': 32}))

    def test_call(self):
        _, _, _, workflow = self.create_example_3_steps_workflow()

        output_value_1 = workflow(p=3)
        self.assertEqual(output_value_1, dict(q=2 * (3 + 1) + 3 * (2 * (3 + 1))))

        output_value_2 = workflow.call(input_values=dict(p=3))
        self.assertEqual(output_value_1, output_value_2)

    def test_from_json_dict(self):
        workflow_json_text = """
        {
            "qualified_name": "my_workflow",
            "header": {
                "description": "My workflow is not too bad."
            },
            "input": {
                "p": {"description": "Input 'p'"}
            },
            "output": {
                "q": {"source": "op3.w", "description": "Output 'q'"}
            },
            "steps": [
                {
                    "id": "op1",
                    "op": "test.core.test_workflow.Op1",
                    "input": {
                        "x": { "source": ".p" }
                    }
                },
                {
                    "id": "op2",
                    "op": "test.core.test_workflow.Op2",
                    "input": {
                        "a": {"source": "op1"}
                    }
                },
                {
                    "id": "op3",
                    "op": "test.core.test_workflow.Op3",
                    "input": {
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
        self.assertEqual(len(workflow.op_meta_info.input), 1)
        self.assertEqual(len(workflow.op_meta_info.output), 1)
        self.assertEqual(workflow.op_meta_info.input['p'], dict(description="Input 'p'"))
        self.assertEqual(workflow.op_meta_info.output['q'], dict(source="op3.w", description="Output 'q'"))

        self.assertEqual(len(workflow.input), 1)
        self.assertEqual(len(workflow.output), 1)

        self.assertIn('p', workflow.input)
        self.assertIn('q', workflow.output)

        self.assertEqual(len(workflow.steps), 3)
        step1 = workflow.steps[0]
        step2 = workflow.steps[1]
        step3 = workflow.steps[2]

        self.assertEqual(step1.id, 'op1')
        self.assertEqual(step2.id, 'op2')
        self.assertEqual(step3.id, 'op3')

        self.assertIs(step1.input.x.source, workflow.input.p)
        self.assertIs(step2.input.a.source, step1.output.y)
        self.assertIs(step3.input.u.source, step1.output.y)
        self.assertIs(step3.input.v.source, step2.output.b)
        self.assertIs(workflow.output.q.source, step3.output.w)

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
        step1 = OpStep(Op1, node_id='op1')
        step2 = OpStep(Op2, node_id='op2')
        step3 = OpStep(Op3, node_id='op3')
        workflow = Workflow(OpMetaInfo('my_workflow', input_dict=OrderedDict(p={}), output_dict=OrderedDict(q={})))
        workflow.add_steps(step1, step2, step3)
        step1.input.x.source = workflow.input.p
        step2.input.a.source = step1.output.y
        step3.input.u.source = step1.output.y
        step3.input.v.source = step2.output.b
        workflow.output.q.source = step3.output.w

        workflow_dict = workflow.to_json_dict()

        expected_json_text = """
        {
            "qualified_name": "my_workflow",
            "header": {},
            "input": {
                "p": {}
            },
            "output": {
                "q": {"source": "op3.w"}
            },
            "steps": [
                {
                    "id": "op1",
                    "op": "test.core.test_workflow.Op1",
                    "input": {
                        "x": { "source": "my_workflow.p" }
                    },
                    "output": {
                        "y": {}
                    }
                },
                {
                    "id": "op2",
                    "op": "test.core.test_workflow.Op2",
                    "input": {
                        "a": {"source": "op1.y"}
                    },
                    "output": {
                        "b": {}
                    }
                },
                {
                    "id": "op3",
                    "op": "test.core.test_workflow.Op3",
                    "input": {
                        "v": {"source": "op2.b"},
                        "u": {"source": "op1.y"}
                    },
                    "output": {
                        "w": {}
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

    def test_repr_svg(self):
        step1 = OpStep(Op1, node_id='op1')
        step2 = OpStep(Op2, node_id='op2')
        step3 = OpStep(Op3, node_id='op3')
        workflow = Workflow(OpMetaInfo('my_workflow', input_dict=OrderedDict(p={}), output_dict=OrderedDict(q={})))
        workflow.add_steps(step1, step2, step3)
        step1.input.x.source = workflow.input.p
        step2.input.a.source = step1.output.y
        step3.input.u.source = step1.output.y
        step3.input.v.source = step2.output.b
        workflow.output.q.source = step3.output.w

        workflow_json = workflow._repr_svg_()
        # print('\n\n%s\n\n' % workflow_json)
        self.assertIsNotNone(workflow_json)


class ExprStepTest(TestCase):
    expression = "dict(x = 1 + 2 * a, y = 3 * b ** 2 + 4 * c ** 3)"

    def test_init(self):
        node = ExprStep(self.expression,
                        OrderedDict([('a', {}), ('b', {}), ('c', {})]),
                        OrderedDict([('x', {}), ('y', {})]),
                        node_id='bibo_8')
        self.assertEqual(node.id, 'bibo_8')
        self.assertEqual(node.expression, self.expression)
        self.assertEqual(str(node),
                         node.id + ' = "dict(x = 1 + 2 * a, y = 3 * b ** 2 + 4 * c ** 3)"(a=None, b=None, c=None) '
                                   '-> (x, y) [ExprStep]')
        self.assertEqual(repr(node), "ExprNode('%s', node_id='bibo_8')" % self.expression)

        node = ExprStep(self.expression)
        self.assertEqual(node.op_meta_info.input, {})
        self.assertEqual(node.op_meta_info.output, {'return': {}})

    def test_from_json_dict(self):
        json_text = """
        {
            "id": "bibo_8",
            "input": {
                "a": {},
                "b": {},
                "c": {}
            },
            "output": {
                "x": {},
                "y": {}
            },
            "expression": "%s"
        }
        """ % self.expression

        json_dict = json.loads(json_text)

        node = ExprStep.from_json_dict(json_dict)

        self.assertIsInstance(node, ExprStep)
        self.assertEqual(node.id, "bibo_8")
        self.assertEqual(node.expression, self.expression)
        self.assertIn('a', node.input)
        self.assertIn('b', node.input)
        self.assertIn('c', node.input)
        self.assertIn('x', node.output)
        self.assertIn('y', node.output)

    def test_to_json_dict(self):
        expected_json_text = """
        {
            "id": "bibo_8",
            "input": {
                "a": {},
                "b": {},
                "c": {}
            },
            "output": {
                "x": {},
                "y": {}
            },
            "expression": "%s"
        }
        """ % self.expression

        step = ExprStep(self.expression, OrderedDict(a={}, b={}, c={}), OrderedDict(x={}, y={}), node_id='bibo_8')
        actual_json_dict = step.to_json_dict()

        actual_json_text = json.dumps(actual_json_dict, indent=4)
        expected_json_dict = json.loads(expected_json_text)
        actual_json_dict = json.loads(actual_json_text)

        self.assertEqual(actual_json_dict, expected_json_dict,
                         msg='\n%sexpected:\n%s\n%s\nbut got:\n%s\n' %
                             (120 * '-', expected_json_text,
                              120 * '-', actual_json_text))

    def test_invoke(self):
        step = ExprStep(self.expression, OrderedDict(a={}, b={}, c={}), OrderedDict(x={}, y={}), node_id='bibo_8')
        a = 1.5
        b = -2.6
        c = 4.3
        step.input.a.value = a
        step.input.b.value = b
        step.input.c.value = c
        step.invoke()
        output_value_x = step.output.x.value
        output_value_y = step.output.y.value
        self.assertEqual(output_value_x, 1 + 2 * a)
        self.assertEqual(output_value_y, 3 * b ** 2 + 4 * c ** 3)

    def test_invoke_from_workflow(self):
        resource = get_resource('workflows/one_expr.json')
        workflow = Workflow.load(resource)
        a = 1.5
        b = -2.6
        c = 4.3
        workflow.input.a.value = a
        workflow.input.b.value = b
        workflow.input.c.value = c
        workflow.invoke()
        output_value_x = workflow.output.x.value
        output_value_y = workflow.output.y.value
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
        self.assertIn('p', step.workflow.input)
        self.assertIn('q', step.workflow.output)

    def test_from_json_dict(self):
        resource = get_resource('workflows/three_ops.json')
        json_text = """
        {
            "id": "workflow_ref_89",
            "workflow": "%s",
            "input": {
                "p": {"value": 2.8}
            }
        }
        """ % resource

        json_dict = json.loads(json_text)

        step = WorkflowStep.from_json_dict(json_dict)

        self.assertIsInstance(step, WorkflowStep)
        self.assertEqual(step.id, "workflow_ref_89")
        self.assertEqual(step.resource, resource)
        self.assertIn('p', step.input)
        self.assertIn('q', step.output)
        self.assertEqual(step.input.p.value, 2.8)
        self.assertEqual(step.output.q.value, None)

        self.assertIsNotNone(step.workflow)
        self.assertIn('p', step.workflow.input)
        self.assertIn('q', step.workflow.output)

        self.assertIs(step.workflow.input.p.source, step.input.p)

    def test_to_json_dict(self):
        resource = get_resource('workflows/three_ops.json')
        workflow = Workflow.load(resource)
        step = WorkflowStep(workflow, resource, node_id='jojo_87')
        actual_json_dict = step.to_json_dict()

        expected_json_text = """
        {
            "id": "jojo_87",
            "workflow": "%s",
            "input": {
                "p": {}
            },
            "output": {
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
        step.input.p.value = 3
        step.invoke(value_cache=value_cache)
        output_value = step.output.q.value
        self.assertEqual(output_value, 2 * (3 + 1) + 3 * (2 * (3 + 1)))
        self.assertEqual(value_cache, {'op1': {'y': 4}, 'op2': {'b': 8}, 'op3': {'w': 32}})

    def test_invoke_as_part_of_workflow(self):
        resource = get_resource('workflows/three_ops.json')
        workflow = Workflow.load(resource)
        step = WorkflowStep(workflow, resource, node_id='jojo_87')

        workflow = Workflow(OpMetaInfo('contains_jojo_87',
                                       has_monitor=True,
                                       input_dict=OrderedDict(x={}),
                                       output_dict=OrderedDict(y={})))
        workflow.add_step(step)
        step.input.p.source = workflow.input.x
        workflow.output.y.source = step.output.q

        from cate.core.workflow import ValueCache
        value_cache = ValueCache()
        workflow.input.x.value = 4
        workflow.invoke(value_cache=value_cache)
        output_value = workflow.output.y.value
        self.assertEqual(output_value, 2 * (4 + 1) + 3 * (2 * (4 + 1)))
        self.assertEqual(value_cache, {'jojo_87.__child__': {'op1': {'y': 5}, 'op2': {'b': 10}, 'op3': {'w': 40}}})


class OpStepTest(TestCase):
    def test_init(self):
        step = OpStep(Op3)

        self.assertRegex(step.id, '^op_step_[0-9a-f]+$')

        self.assertTrue(len(step.input), 2)
        self.assertTrue(len(step.output), 1)

        self.assertTrue(hasattr(step.input, 'u'))
        self.assertIs(step.input.u.node, step)
        self.assertEqual(step.input.u.name, 'u')

        self.assertTrue(hasattr(step.input, 'v'))
        self.assertIs(step.input.v.node, step)
        self.assertEqual(step.input.v.name, 'v')

        self.assertTrue(hasattr(step.output, 'w'))
        self.assertIs(step.output.w.node, step)
        self.assertEqual(step.output.w.name, 'w')

        self.assertEqual(str(step), step.id + ' = test.core.test_workflow.Op3(u=None, v=None) -> (w) [OpStep]')
        self.assertEqual(repr(step), "OpStep(test.core.test_workflow.Op3, node_id='%s')" % step.id)

    def test_init_operation_and_name_are_equivalent(self):
        step3 = OpStep(Op3)
        self.assertIsNotNone(step3.op)
        self.assertIsNotNone(step3.op_meta_info)
        node31 = OpStep(object_to_qualified_name(Op3))
        self.assertIs(node31.op, step3.op)
        self.assertIs(node31.op_meta_info, step3.op_meta_info)

    def test_invoke(self):
        step1 = OpStep(Op1)
        step1.input.x.value = 3
        step1.invoke()
        output_value = step1.output.y.value
        self.assertEqual(output_value, 3 + 1)

        step2 = OpStep(Op2)
        step2.input.a.value = 3
        step2.invoke()
        output_value = step2.output.b.value
        self.assertEqual(output_value, 2 * 3)

        step3 = OpStep(Op3)
        step3.input.u.value = 4
        step3.input.v.value = 5
        step3.invoke()
        output_value = step3.output.w.value
        self.assertEqual(output_value, 2 * 4 + 3 * 5)

    def test_call(self):
        step1 = OpStep(Op1)
        step1.input.x.value = 3
        output_value = step1(x=3)
        self.assertEqual(output_value, dict(y=3 + 1))

        step2 = OpStep(Op2)
        output_value = step2(a=3)
        self.assertEqual(output_value, dict(b=2 * 3))

        step3 = OpStep(Op3)
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
        step1 = OpStep(Op1)
        step2 = OpStep(Op2)
        step3 = OpStep(Op3)
        step2.input.a.source = step1.output.y
        step3.input.u.source = step1.output.y
        step3.input.v.source = step2.output.b
        self.assertConnectionsAreOk(step1, step2, step3)

        with self.assertRaises(AttributeError) as cm:
            step1.input.a.source = step3.input.u
        self.assertEqual(str(cm.exception), "attribute 'a' not found")

    def test_disconnect_source(self):
        step1 = OpStep(Op1)
        step2 = OpStep(Op2)
        step3 = OpStep(Op3)

        step2.input.a.source = step1.output.y
        step3.input.u.source = step1.output.y
        step3.input.v.source = step2.output.b
        self.assertConnectionsAreOk(step1, step2, step3)

        step3.input.v.source = None

        self.assertIs(step2.input.a.source, step1.output.y)
        self.assertIs(step3.input.u.source, step1.output.y)

        step2.input.a.source = None

        self.assertIs(step3.input.u.source, step1.output.y)
        self.assertIs(step3.input.u.source, step1.output.y)

        step3.input.u.source = None

    def assertConnectionsAreOk(self, step1, step2, step3):
        self.assertIs(step2.input.a.source, step1.output.y)
        self.assertIs(step3.input.u.source, step1.output.y)
        self.assertIs(step3.input.v.source, step2.output.b)

    def test_from_json_dict_value(self):
        json_text = """
        {
            "id": "op3",
            "op": "test.core.test_workflow.Op3",
            "input": {
                "u": {"value": 647},
                "v": {"value": 2.9}
            }
        }
        """

        json_dict = json.loads(json_text)

        step3 = OpStep.from_json_dict(json_dict)

        self.assertIsInstance(step3, OpStep)
        self.assertEqual(step3.id, "op3")
        self.assertIsInstance(step3.op, OpRegistration)
        self.assertIn('u', step3.input)
        self.assertIn('v', step3.input)
        self.assertIn('w', step3.output)

        self.assertEqual(step3.input.u.value, 647)
        self.assertEqual(step3.input.v.value, 2.9)

    def test_from_json_dict_source(self):
        json_text = """
        {
            "id": "op3",
            "op": "test.core.test_workflow.Op3",
            "input": {
                "u": {"source": "stat_op.stats"},
                "v": {"source": ".latitude"}
            }
        }
        """

        json_dict = json.loads(json_text)

        step3 = OpStep.from_json_dict(json_dict)

        self.assertIsInstance(step3, OpStep)
        self.assertEqual(step3.id, "op3")
        self.assertIsInstance(step3.op, OpRegistration)
        self.assertIn('u', step3.input)
        self.assertIn('v', step3.input)
        self.assertIn('w', step3.output)
        self.assertEqual(step3.input.u._source_ref, ('stat_op', 'stats'))
        self.assertEqual(step3.input.u.source, None)
        self.assertEqual(step3.input.v._source_ref, (None, 'latitude'))
        self.assertEqual(step3.input.v.source, None)

    def test_to_json_dict(self):
        step3 = OpStep(Op3, node_id='op3')
        step3.input.u.value = 2.8

        step3_dict = step3.to_json_dict()

        expected_json_text = """
        {
            "id": "op3",
            "op": "test.core.test_workflow.Op3",
            "input": {
                "v": {},
                "u": {"value": 2.8}
            },
            "output": {
                "w": {}
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
        step3.input.u.value = 2.8
        step3.input.v.value = 1.2
        step3.invoke()
        step3_dict = step3.to_json_dict()

        expected_json_text = """
        {
            "id": "op3",
            "op": "test.core.test_workflow.Op3",
            "input": {
                "v": {"value": 1.2},
                "u": {"value": 2.8}
            },
            "output": {
                "w": {}
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
        step = NoOpStep(input_dict=OrderedDict([('a', {}), ('b', {})]),
                        output_dict=OrderedDict([('c', {}), ('d', {})]))

        self.assertRegex(step.id, '^no_op_step_[0-9a-f]+$')

        self.assertIsNotNone(step.op_meta_info)
        self.assertEqual(step.op_meta_info.qualified_name, step.id)

        self.assertTrue(len(step.input), 2)
        self.assertTrue(len(step.output), 2)

        self.assertTrue(hasattr(step.input, 'a'))
        self.assertIs(step.input.a.node, step)

        self.assertTrue(hasattr(step.output, 'd'))
        self.assertIs(step.output.d.node, step)

        self.assertEqual(str(step), step.id + ' = noop(a=None, b=None) -> (c, d) [NoOpStep]')
        self.assertEqual(repr(step), "NoOpStep(node_id='%s')" % step.id)

    def test_invoke(self):
        step = NoOpStep(input_dict=OrderedDict([('a', {}), ('b', {})]),
                        output_dict=OrderedDict([('c', {}), ('d', {})]))

        # Operation: Swap input
        step.output.c.source = step.input.b
        step.output.d.source = step.input.a

        step.input.a.value = 'A'
        step.input.b.value = 'B'
        step.invoke()
        self.assertEqual(step.output.c.value, 'B')
        self.assertEqual(step.output.d.value, 'A')

    def test_from_and_to_json(self):
        json_text = """
        {
            "id": "op3",
            "no_op": true,
            "input": {
                "a": {"value": 647},
                "b": {"value": 2.9}
            },
            "output": {
                "c": {"source": "op3.b"},
                "d": {"source": "op3.a"}
            }
        }
        """

        json_dict = json.loads(json_text)

        step = NoOpStep.from_json_dict(json_dict)

        self.assertIsInstance(step, NoOpStep)
        self.assertEqual(step.id, "op3")
        self.assertIn('a', step.input)
        self.assertIn('b', step.input)
        self.assertIn('c', step.output)
        self.assertIn('d', step.output)

        self.assertEqual(step.input.a.value, 647)
        self.assertEqual(step.input.b.value, 2.9)
        self.assertEqual(step.output.c._source_ref, ('op3', 'b'))
        self.assertEqual(step.output.d._source_ref, ('op3', 'a'))

        json_dict_2 = step.to_json_dict()
        # self.assertEqual(json_dict, json_dict_2)


class SubProcessStepTest(TestCase):
    def test_init(self):
        step = SubProcessStep(['cd', '{{dir}}'],
                              input_dict=OrderedDict(dir=dict(data_type=str)))

        self.assertRegex(step.id, '^sub_process_step_[0-9a-f]+$')

        self.assertIsNotNone(step.op_meta_info)
        self.assertEqual(step.op_meta_info.qualified_name, step.id)

        self.assertTrue(len(step.input), 1)
        self.assertTrue(len(step.output), 1)

        self.assertTrue(hasattr(step.input, 'dir'))
        self.assertIs(step.input.dir.node, step)

        self.assertTrue(hasattr(step.output, 'return'))
        self.assertIs(step.output['return'].node, step)

        self.assertEqual(str(step), step.id + ' = "cd {{dir}}"(dir=None) [SubProcessStep]')
        self.assertEqual(repr(step), "SubProcessStep(['cd', '{{dir}}'], node_id='%s')" % step.id)

    def test_invoke(self):
        step = SubProcessStep(['cd', '{{dir}}'],
                              input_dict=OrderedDict([('dir', dict(data_type=str))]))

        step.input.dir.value = '..'

        step.invoke()
        self.assertEqual(step.output['return'].value, 0)

    def test_from_and_to_json(self):
        json_text = """
        {
            "id": "op3",
            "sub_process_arguments": ["cd", "{{dir}}"],
            "input": {
                "dir": {"value": "."}
            }
        }
        """

        json_dict = json.loads(json_text)

        step = SubProcessStep.from_json_dict(json_dict)

        self.assertIsInstance(step, SubProcessStep)
        self.assertEqual(step.id, "op3")
        self.assertIn('dir', step.input)
        self.assertIn('return', step.output)
        self.assertEqual(step.input.dir.value, '.')

        json_dict_2 = step.to_json_dict()
        # self.assertEqual(json_dict, json_dict_2)


class NodePortTest(TestCase):
    def test_init(self):
        step = OpStep(Op1, node_id='myop')
        source = NodePort(step, 'x')

        self.assertIs(source.node, step)
        self.assertEqual(source.node_id, 'myop')
        self.assertEqual(source.name, 'x')
        self.assertEqual(source.source, None)
        self.assertEqual(source.value, None)
        self.assertEqual(str(source), 'myop.x')
        self.assertEqual(repr(source), "NodePort('myop', 'x')")

    def test_resolve_source_ref(self):
        step1 = OpStep(Op1, node_id='myop1')
        step2 = OpStep(Op2, node_id='myop2')
        step2.input.a._source_ref = ('myop1', 'y')

        g = Workflow(OpMetaInfo('myWorkflow',
                                has_monitor=True,
                                input_dict=OrderedDict(x={}),
                                output_dict=OrderedDict(b={})))
        g.add_steps(step1, step2)

        step2.input.a.update_source()

        self.assertEqual(step2.input.a._source_ref, ('myop1', 'y'))
        self.assertIs(step2.input.a.source, step1.output.y)
        self.assertIs(step2.input.a.value, None)

    def test_from_json_dict(self):
        step2 = OpStep(Op2, node_id='myop2')
        port2 = NodePort(step2, 'a')

        port2.from_json_dict(json.loads('{"a": {"value": 2.6}}'))
        self.assertEqual(port2._source_ref, None)
        self.assertEqual(port2._source, None)
        self.assertEqual(port2._value, 2.6)

        port2.from_json_dict(json.loads('{"a": {"source": "myop1.y"}}'))
        self.assertEqual(port2._source_ref, ('myop1', 'y'))
        self.assertEqual(port2._source, None)
        self.assertEqual(port2._value, None)

        # "myop1.y" is a shorthand for {"source": "myop1.y"}
        port2.from_json_dict(json.loads('{"a": "myop1.y"}'))
        self.assertEqual(port2._source_ref, ('myop1', 'y'))
        self.assertEqual(port2._source, None)
        self.assertEqual(port2._value, None)

        port2.from_json_dict(json.loads('{"a": {"source": ".y"}}'))
        self.assertEqual(port2._source_ref, (None, 'y'))
        self.assertEqual(port2._source, None)
        self.assertEqual(port2._value, None)

        # ".x" is a shorthand for {"source": ".x"}
        port2.from_json_dict(json.loads('{"a": ".y"}'))
        self.assertEqual(port2._source_ref, (None, 'y'))
        self.assertEqual(port2._source, None)
        self.assertEqual(port2._value, None)

        # "myop1" is a shorthand for {"source": "myop1"}
        port2.from_json_dict(json.loads('{"a": "myop1"}'))
        self.assertEqual(port2._source_ref, ('myop1', None))
        self.assertEqual(port2._source, None)
        self.assertEqual(port2._value, None)

        # if "a" is defined, but neither "source" nor "value" is given, it will neither have a source nor a value
        port2.from_json_dict(json.loads('{"a": {}}'))
        self.assertEqual(port2._source_ref, None)
        self.assertEqual(port2._source, None)
        self.assertEqual(port2._value, None)
        port2.from_json_dict(json.loads('{"a": null}'))
        self.assertEqual(port2._source_ref, None)
        self.assertEqual(port2._source, None)
        self.assertEqual(port2._value, None)

        # if "a" is not defined at all, it will neither have a source nor a value
        port2.from_json_dict(json.loads('{}'))
        self.assertEqual(port2._source_ref, None)
        self.assertEqual(port2._source, None)
        self.assertEqual(port2._value, None)

        with self.assertRaises(ValueError) as cm:
            port2.from_json_dict(json.loads('{"a": {"value": 2.6, "source": "y"}}'))
        self.assertEqual(str(cm.exception),
                         "error decoding 'myop2.a' because \"source\" and \"value\" are mutually exclusive")

        expected_msg = "error decoding 'myop2.a' because the \"source\" value format is " \
                       "neither \"<node-id>.<name>\", \"<node-id>\", nor \".<name>\""

        with self.assertRaises(ValueError) as cm:
            port2.from_json_dict(json.loads('{"a": {"source": ""}}'))
        self.assertEqual(str(cm.exception), expected_msg)

        with self.assertRaises(ValueError) as cm:
            port2.from_json_dict(json.loads('{"a": {"source": "."}}'))
        self.assertEqual(str(cm.exception), expected_msg)

        with self.assertRaises(ValueError) as cm:
            port2.from_json_dict(json.loads('{"a": {"source": "var."}}'))
        self.assertEqual(str(cm.exception), expected_msg)

    def test_to_json_dict(self):
        step1 = OpStep(Op1, node_id='myop1')
        step2 = OpStep(Op2, node_id='myop2')

        self.assertEqual(step2.input.a.to_json_dict(), dict())

        step2.input.a.value = 982
        self.assertEqual(step2.input.a.to_json_dict(), dict(value=982))

        step2.input.a.source = step1.output.y
        self.assertEqual(step2.input.a.to_json_dict(), dict(source='myop1.y'))

        step2.input.a.source = None
        self.assertEqual(step2.input.a.to_json_dict(), dict())
