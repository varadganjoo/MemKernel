"""Private key/policy permissions using the host's filesystem access controls."""

from __future__ import annotations

import os
from pathlib import Path


def _windows_identities():
    import win32api
    import win32con
    import win32security

    token = win32security.OpenProcessToken(
        win32api.GetCurrentProcess(), win32con.TOKEN_QUERY
    )
    try:
        user = win32security.GetTokenInformation(token, win32security.TokenUser)[0]
    finally:
        token.Close()
    return [
        user,
        win32security.ConvertStringSidToSid("S-1-5-18"),
        win32security.ConvertStringSidToSid("S-1-5-32-544"),
    ]


def assert_private_permissions(path: Path, *, fd: int | None = None) -> None:
    """Refuse broad content access or ACL modification by other OS identities."""
    if os.name != "nt":
        info = os.fstat(fd) if fd is not None else path.stat()
        if info.st_mode & 0o077 or info.st_uid not in {os.geteuid(), 0}:
            raise ValueError("private-file permissions must be 0600 or stricter")
        return
    import win32security

    allowed = {
        win32security.ConvertSidToStringSid(sid) for sid in _windows_identities()
    }
    information = (
        win32security.DACL_SECURITY_INFORMATION
        | win32security.OWNER_SECURITY_INFORMATION
    )
    if fd is not None:
        import msvcrt

        descriptor = win32security.GetSecurityInfo(
            msvcrt.get_osfhandle(fd), win32security.SE_FILE_OBJECT, information
        )
    else:
        descriptor = win32security.GetFileSecurity(str(path), information)
    owner_sid = descriptor.GetSecurityDescriptorOwner()
    if owner_sid is None:
        raise ValueError("private-file permissions have no verifiable owner")
    owner = win32security.ConvertSidToStringSid(owner_sid)
    if owner not in allowed:
        raise ValueError("private-file permissions have an untrusted owner")
    acl = descriptor.GetSecurityDescriptorDacl()
    if acl is None:
        raise ValueError("private-file permissions must deny unrestricted access")
    for i in range(acl.GetAceCount()):
        ace = acl.GetAce(i)
        if ace[0][0] == win32security.ACCESS_DENIED_ACE_TYPE:
            continue
        if ace[0][0] != win32security.ACCESS_ALLOWED_ACE_TYPE:
            raise ValueError(
                "private-file permissions contain an unsupported access rule"
            )
        sid = win32security.ConvertSidToStringSid(ace[-1])
        # Read/write data, delete, generic access, or the ability to change the ACL/owner.
        if sid not in allowed and ace[1] & 0xF00D0017:
            raise ValueError(
                "private-file permissions grant another identity content or control access"
            )


def protect_private_file(path: Path) -> None:
    """Apply before writing secrets. The caller must also protect parent directories."""
    if os.name != "nt":
        path.chmod(0o600)
        return
    import win32security

    acl = win32security.ACL()
    for sid in _windows_identities():
        acl.AddAccessAllowedAce(win32security.ACL_REVISION, 0x1F01FF, sid)
    win32security.SetNamedSecurityInfo(
        str(path),
        win32security.SE_FILE_OBJECT,
        win32security.DACL_SECURITY_INFORMATION
        | win32security.PROTECTED_DACL_SECURITY_INFORMATION,
        None,
        None,
        acl,
        None,
    )
    assert_private_permissions(path)
