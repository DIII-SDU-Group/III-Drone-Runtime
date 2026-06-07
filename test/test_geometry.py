from math import isclose

from iii_drone_runtime.geometry import (
    EulerAngles,
    Quaternion,
    euler_to_quaternion,
    quaternion_inverse,
    quaternion_multiply,
    quaternion_to_euler,
    rotation_matrix_from_euler,
)


def _assert_close(actual: float, expected: float, tolerance: float = 1e-6):
    assert isclose(actual, expected, abs_tol=tolerance)


def test_euler_quaternion_round_trip():
    euler = EulerAngles(roll=0.2, pitch=-0.1, yaw=0.5)

    actual = quaternion_to_euler(euler_to_quaternion(euler))

    _assert_close(actual.roll, euler.roll)
    _assert_close(actual.pitch, euler.pitch)
    _assert_close(actual.yaw, euler.yaw)


def test_quaternion_inverse_product_is_identity():
    quaternion = euler_to_quaternion(EulerAngles(roll=-0.4, pitch=0.1, yaw=0.3))

    actual = quaternion_multiply(quaternion, quaternion_inverse(quaternion))

    _assert_close(actual.w, 1.0)
    _assert_close(actual.x, 0.0)
    _assert_close(actual.y, 0.0)
    _assert_close(actual.z, 0.0)


def test_rotation_matrix_identity():
    matrix = rotation_matrix_from_euler(EulerAngles(roll=0.0, pitch=0.0, yaw=0.0))

    assert matrix == (
        (1.0, 0.0, 0.0),
        (0.0, 1.0, 0.0),
        (-0.0, 0.0, 1.0),
    )


def test_geometry_module_is_ros_and_core_free():
    quaternion = Quaternion(w=1.0, x=0.0, y=0.0, z=0.0)

    assert quaternion_to_euler(quaternion) == EulerAngles(roll=0.0, pitch=0.0, yaw=0.0)
