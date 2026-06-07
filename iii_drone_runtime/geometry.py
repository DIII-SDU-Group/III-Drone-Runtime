"""Small ROS-free geometry helpers for runtime API data shaping."""

from __future__ import annotations

from dataclasses import dataclass
from math import asin, atan2, cos, sin


@dataclass(frozen=True)
class Quaternion:
    w: float
    x: float
    y: float
    z: float


@dataclass(frozen=True)
class EulerAngles:
    roll: float
    pitch: float
    yaw: float


def euler_to_quaternion(euler: EulerAngles) -> Quaternion:
    cr = cos(euler.roll * 0.5)
    sr = sin(euler.roll * 0.5)
    cp = cos(euler.pitch * 0.5)
    sp = sin(euler.pitch * 0.5)
    cy = cos(euler.yaw * 0.5)
    sy = sin(euler.yaw * 0.5)

    return Quaternion(
        w=cr * cp * cy + sr * sp * sy,
        x=sr * cp * cy - cr * sp * sy,
        y=cr * sp * cy + sr * cp * sy,
        z=cr * cp * sy - sr * sp * cy,
    )


def quaternion_to_euler(quaternion: Quaternion) -> EulerAngles:
    roll = atan2(
        2.0 * (quaternion.w * quaternion.x + quaternion.y * quaternion.z),
        1.0 - 2.0 * (quaternion.x * quaternion.x + quaternion.y * quaternion.y),
    )
    pitch_argument = 2.0 * (quaternion.w * quaternion.y - quaternion.z * quaternion.x)
    pitch_argument = max(-1.0, min(1.0, pitch_argument))
    pitch = asin(pitch_argument)
    yaw = atan2(
        2.0 * (quaternion.w * quaternion.z + quaternion.x * quaternion.y),
        1.0 - 2.0 * (quaternion.y * quaternion.y + quaternion.z * quaternion.z),
    )
    return EulerAngles(roll=roll, pitch=pitch, yaw=yaw)


def quaternion_multiply(left: Quaternion, right: Quaternion) -> Quaternion:
    return Quaternion(
        w=left.w * right.w - left.x * right.x - left.y * right.y - left.z * right.z,
        x=left.w * right.x + left.x * right.w + left.y * right.z - left.z * right.y,
        y=left.w * right.y - left.x * right.z + left.y * right.w + left.z * right.x,
        z=left.w * right.z + left.x * right.y - left.y * right.x + left.z * right.w,
    )


def quaternion_inverse(quaternion: Quaternion) -> Quaternion:
    return Quaternion(
        w=quaternion.w,
        x=-quaternion.x,
        y=-quaternion.y,
        z=-quaternion.z,
    )


def rotation_matrix_from_euler(euler: EulerAngles) -> tuple[tuple[float, float, float], ...]:
    cos_yaw = cos(euler.yaw)
    cos_pitch = cos(euler.pitch)
    cos_roll = cos(euler.roll)
    sin_yaw = sin(euler.yaw)
    sin_pitch = sin(euler.pitch)
    sin_roll = sin(euler.roll)

    return (
        (
            cos_pitch * cos_yaw,
            sin_roll * sin_pitch * cos_yaw - cos_roll * sin_yaw,
            cos_roll * sin_pitch * cos_yaw + sin_roll * sin_yaw,
        ),
        (
            cos_pitch * sin_yaw,
            sin_roll * sin_pitch * sin_yaw + cos_roll * cos_yaw,
            cos_roll * sin_pitch * sin_yaw - sin_roll * cos_pitch,
        ),
        (
            -sin_pitch,
            sin_roll * cos_pitch,
            cos_roll * cos_pitch,
        ),
    )
