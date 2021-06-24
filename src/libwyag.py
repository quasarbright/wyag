import argparse
import collections
import configparser
import hashlib
import os
import re
from typing import Optional

import sys
import zlib

argparser = argparse.ArgumentParser(description="The stupid content tracker")
argsubparsers = argparser.add_subparsers(title="Commands", dest="command")
argsubparsers.required = True

argsp = argsubparsers.add_parser("init", help="Initialize a new, empty repository")
argsp.add_argument("path",
                   metavar="directory",
                   nargs="?",
                   default=".",
                   help="Where to create the repository")


def cmd_init(args):
    repo_create(args.path)


argsp = argsubparsers.add_parser("cat-file", help="Provide content of repository objects")
argsp.add_argument("type",
                   metavar="type",
                   choices=['blob', 'commit', 'tag', 'tree'],
                   help='Specify the object type')
argsp.add_argument('object',
                   metavar='object',
                   help='The object to display')


def cmd_cat_file(args):
    repo = repo_find()
    cat_file(repo, args.object, fmt=args.type.encode())


def cat_file(repo, obj, fmt=None):
    obj = object_read(repo, object_find(repo, obj, fmt=fmt))
    sys.stdout.buffer.write(obj.serialize())


argsp = argsubparsers.add_parser("hash-object",
                                 help="Compute object ID and optionally create blob from file")
argsp.add_argument("-t",
                   metavar="type",
                   dest="type",
                   choices=["blob", "commit", "tag", "tree"],
                   default="blob",
                   help="Specify the type")
argsp.add_argument("-w",
                   dest="write",
                   action="store_true",
                   help="Actually write the object into the database")
argsp.add_argument("path",
                   help="Read object from <file>")


def cmd_hash_object(args):
    if args.write:
        repo = repo_find()
    else:
        repo = None
    with open(args.path, 'rb') as f:
        sha = object_hash(f, args.type.encode('ascii'), repo is not None)
        print(sha)


class GitRepository:
    """A git repository"""

    worktree = None
    gitdir = None
    conf = None

    def __init__(self, path, force=False):
        self.worktree = path
        self.gitdir = os.path.join(path, ".git")

        if not (force or os.path.isdir):
            raise Exception("Not a Git repository")

        self.conf = configparser.ConfigParser()
        cf = repo_file(self, "config")

        if cf and os.path.exists(cf):
            self.conf.read([cf])
        elif not force:
            raise Exception("Configuration file missing")

        if not force:
            vers = int(self.conf.get("core", "repositoryformatversion"))
            if vers != 0:
                raise Exception(f"Unsupported repositoryformatversion {vers}")


def repo_path(repo, *path):
    """Compute path under repo's gitdir."""
    return os.path.join(repo.gitdir, *path)


def repo_file(repo, *path, mkdir=False):
    """Same as repo_path, but create dirname(*path) if absent.  For
    example, repo_file(r, \"refs\", \"remotes\", \"origin\", \"HEAD\") will create
    .git/refs/remotes/origin."""

    if repo_dir(repo, *path[:-1], mkdir=mkdir):
        return repo_path(repo, *path)


def repo_dir(repo, *path, mkdir=False):
    """Same as repo_path, but mkdir *path if absent if mkdir."""

    path = repo_path(repo, *path)

    if os.path.exists(path):
        if os.path.isdir(path):
            return path
        else:
            raise Exception("Not a directory %s" % path)

    if mkdir:
        os.makedirs(path)
        return path
    else:
        return None


def repo_create(path):
    """Create repo at path"""

    repo = GitRepository(path, force=True)

    if os.path.exists(repo.worktree):
        if not os.path.isdir(repo.worktree):
            raise Exception("%s is not a directory!" % path)
        if os.listdir(repo.worktree):
            raise Exception("%s is not empty!" % path)
    else:
        os.makedirs(repo.worktree)

    repo_dir(repo, "branches", mkdir=True)
    repo_dir(repo, "objects", mkdir=True)
    repo_dir(repo, "refs", "tags", mkdir=True)
    repo_dir(repo, "refs", "heads", mkdir=True)

    with open(repo_file(repo, "description"), "w") as f:
        f.write("Unnamed repository; edit this file 'description' to name the repository.\n")

    with open(repo_file(repo, "HEAD"), "w") as f:
        f.write("ref: refs/heads/master\n")

    with open(repo_file(repo, "config"), "w") as f:
        config = repo_default_config()
        config.write(f)

    return repo


def repo_default_config():
    config = configparser.ConfigParser()
    config.add_section("core")
    config.set("core", "repositoryformatversion", "0")
    config.set("core", "filemode", "false")
    config.set("core", "bare", "false")
    return config


def repo_find(path=".", required=True) -> Optional[GitRepository]:
    path = os.path.realpath(path)

    if os.path.isdir(os.path.join(path, ".git")):
        return GitRepository(path)

    parent = os.path.realpath(os.path.join(path, ".."))
    if parent == path:
        if required:
            raise Exception("Unable to find .git directory.")
        else:
            return None
    else:
        return repo_find(parent, required)


class GitObject:
    fmt: Optional[bytes] = None

    def __init__(self, repo: GitRepository, data: Optional[bytes] = None):
        self.repo = repo

        if data is not None:
            self.deserialize(data)

    def serialize(self):
        """Reads from self.data and convert to a meaningful representation"""
        raise NotImplemented

    def deserialize(self, data):
        raise NotImplemented


class GitBlob(GitObject):
    fmt = b'blob'

    def serialize(self):
        return self.blob_data

    def deserialize(self, data):
        self.blob_data = data


cls_map = {
    b'commit': GitCommit,
    b'tree': GitTree,
    b'tag': GitTag,
    b'blob': GitBlob
}


def object_read(repo, sha: str) -> GitObject:
    path = repo_file(repo, "objects", sha[:2], sha[2:])

    with open(path, "rb") as f:
        raw = zlib.decompress(f.read())

        # the first few bytes specify the format
        fmt_index = raw.find(b' ')
        fmt = raw[:fmt_index]

        # after the format comes the size
        size_index = raw.find(b'\x00', fmt_index)
        size = int(raw[fmt_index:size_index])
        if size != len(raw) - size_index - 1:
            raise Exception(f"Malformed object {sha}: bad length")

        cls = cls_map.get(fmt)
        if cls is None:
            raise Exception(f"Unknown type {fmt.decode('ascii')} for object {sha}")
        # everything after size is the data
        return cls(repo, raw[size_index + 1:])


def object_find(repo, name, fmt=None, follow=True):
    return name


def object_write(obj: GitObject, actually_write=True):
    data = obj.serialize()
    result = obj.fmt + b' ' + str(len(data)).encode('ascii') + b'\x00' + data
    sha = hashlib.sha1(result).hexdigest()
    if actually_write:
        path = repo_file(obj.repo, "objects", sha[:2], sha[2:], mkdir=actually_write)
        with open(path, "wb") as f:
            f.write(zlib.compress(result))
    return sha


def object_hash(f, fmt, repo=None):
    data = f.read()

    cls = cls_map.get(fmt)
    if cls is None:
        raise Exception(f"Unknown type: {fmt}")
    obj = cls(repo, data)
    return object_write(obj, repo is not None)


def main(argv=sys.argv[1:]):
    args = argparser.parse_args(argv)

    if args.command == "add":
        cmd_add(args)
    elif args.command == "cat-file":
        cmd_cat_file(args)
    elif args.command == "checkout":
        cmd_checkout(args)
    elif args.command == "commit":
        cmd_commit(args)
    elif args.command == "hash-object":
        cmd_hash_object(args)
    elif args.command == "init":
        cmd_init(args)
    elif args.command == "log":
        cmd_log(args)
    elif args.command == "ls-tree":
        cmd_ls_tree(args)
    elif args.command == "merge":
        cmd_merge(args)
    elif args.command == "rebase":
        cmd_rebase(args)
    elif args.command == "rev-parse":
        cmd_rev_parse(args)
    elif args.command == "rm":
        cmd_rm(args)
    elif args.command == "show-ref":
        cmd_show_ref(args)
    elif args.command == "tag":
        cmd_tag(args)
