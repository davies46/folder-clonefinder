from __future__ import with_statement

import argparse
import contextlib
import operator
import os
import re
import threading
import time

lock = threading.Lock()
threads = {}
paths = {}
sizes = {}
# mounted_filesystems = ['/', '/media/home', '/media/UserData', '/media/win7backup', '/media/4tb-ext', '/media/pdavies/SeagateV2', '/media/pdavies/Hitachi']
# mounted_filesystems = ['/media/4tb-ext',  '/media/pdavies/Hitachi']
subtree_visit_num = 0
no_access = []
not_found = []
not_folder = []

units = {"B": 1, "KB": 10 ** 3, "MB": 10 ** 6, "GB": 10 ** 9, "TB": 10 ** 12, "K": 10 ** 3, "M": 10 ** 6, "G": 10 ** 9, "T": 10 ** 12}


def threadName():
    return threads[threading.get_ident()].getName()


class Args:
    def __init__(self):
        parser = argparse.ArgumentParser('Find duplicate folders in filesystem')
        # parser.add_argument('infile', metavar='input file', nargs=1, help='File to process')
        parser.add_argument('-e', '--exclude', default=None, help='Comma separated list of root-level folders to exclude')
        parser.add_argument('-m', '--minsize', default='5G', help='Smallest size to care about')
        parser.add_argument('-x', '--exclude-subfolders', default='/media/pdavies/Hitachi/timeshift,/proc,/run', help='Folders to exclude from search')

        parser_args = parser.parse_args()
        # print(parser_args)
        if parser_args.exclude:
            self.exclude = parser_args.exclude.split(',')
        else:
            self.exclude = []
        if parser_args.exclude_subfolders:
            self.exclude_subfolders = parser_args.exclude_subfolders.split(',')
        else:
            self.exclude_subfolders = []

        self.minsize = Args.parse_size(parser_args.minsize)

    @staticmethod
    def parse_size(size):
        # print('Parse', size)
        size = size.upper()
        if not size.endswith('B'):
            size += 'B'
        if not re.match(r' ', size):
            size = re.sub(r'([KMGT]?B)', r' \1', size)
        number, unit = [string.strip() for string in size.split()]
        return int(float(number) * units[unit])


args = Args()
print('Min folder size', args.minsize)
print('Exclude root folders', args.exclude)
exclude_folders = args.exclude
exclude_subfolders = args.exclude_subfolders

print("Filesystem\tMounted on\tUse%\tIUse%")
mounted_filesystems = {}
nonroot_filesystems = []

with contextlib.closing(open('/etc/mtab')) as fp:
    for m in fp:
        fs_spec, fs_file, fs_vfstype, fs_mntops, fs_freq, fs_passno = m.split()
        if fs_spec.startswith('/') and '/loop' not in fs_spec:
            try:
                r = os.statvfs(fs_file)
                if r.f_files > 0:
                    block_usage_pct = 100.0 - (float(r.f_bavail) / float(r.f_blocks) * 100)
                    inode_usage_pct = 100.0 - (float(r.f_favail) / float(r.f_files) * 100)
                    if fs_file in exclude_folders:
                        print('Exclude drive folder', fs_file)
                    else:
                        if fs_spec not in mounted_filesystems:
                            mounted_filesystems[fs_spec] = fs_file
                            if fs_file != '/':
                                nonroot_filesystems.append(fs_file)
                            print("%s\t%s\t\t%d%%\t%d%%" % (fs_spec, fs_file, block_usage_pct, inode_usage_pct))
            except PermissionError:
                pass


def isChild(p1, p2):
    return p1.startswith(p2) and p1 != p2


class Duplicate:
    def __init__(self, size, path1, path2):
        self.size = size
        if path1 > path2:
            self.__path1 = path1
            self.__path2 = path2
        else:
            self.__path1 = path1
            self.__path2 = path2

    def __str__(self):
        return '%d, <%s> and <%s>' % (self.size, self.__path1, self.__path2)

    def getKey(self):
        return self.__path1 + '+' + self.__path2

    def anyendswith(self, other_dupe, string):
        return self.__path1.endswith(string) or self.__path2.endswith(string) or other_dupe.__path1.endswith(string) or other_dupe.__path2.endswith(string)

    def isChildOf(self, other_dupe):
        # Either path1 or path2 of either dupe might be a different device, so a different root, but they must have a root in common
        # so if we compare all permutations we should catch it
        # Comparison is a,b with c,d so a child would mean 'a' is a child of 'c' and 'b' of 'd' OR
        # 'a' of 'd' and 'b' of 'c'
        # if self.anyendswith(other_dupe, 'Win7Pro-disk3.vmdk'):
        #     print('Yeah')
        we_are_child = isChild(self.__path1, other_dupe.__path1) and isChild(self.__path2, other_dupe.__path2)
        we_are_child = we_are_child or (isChild(self.__path1, other_dupe.__path2) and isChild(self.__path2, other_dupe.__path1))
        return we_are_child


duplicates = []


def searchTree(path):
    # What if here we treat files and folders uniformly?
    global subtree_visit_num
    if path in exclude_subfolders:
        print('Path is in subfolders to exclude:', path)
        return 0, 0

    if threadName() == '/':
        # print('root')
        if any(substring in path for substring in nonroot_filesystems):
            print("Skip %s whilst searcing '/', it's on another drive" % path)
            return 0, 0

    total_size = 0
    digest = 0
    subtree_visit_num += 1
    if subtree_visit_num % 100000 == 0:
        # if len(path) > 100:
        #     vp = path[0:49] + '...' + path[-49:]
        # else:
        #     vp = path
        print('Visiting subtree #%d. Paths added: %d. Duplicates: %d. %s' % (subtree_visit_num, len(paths), len(duplicates), threadName()))

    # At this level we're assuming this is a folder, and we've ensured we're only calling this for folders
    try:
        # Examine every file and folder at this level
        with os.scandir(path) as dir_entries:
            for dir_entry in dir_entries:
                # Ignore symbolic links
                if 'smb-share:server' in dir_entry.path:
                    continue
                if dir_entry.is_symlink():
                    continue

                basename = os.path.basename(dir_entry.path)
                filedigest = hash(basename)
                digest += filedigest
                if dir_entry.is_file():
                    filesize = os.path.getsize(dir_entry.path)
                    total_size += filesize
                    # Also add in the file size as an extra indicator of similarity when comparing with cousins
                    digest += filesize
                    # However, if this file itself is huge, it earns its own entry
                    if filesize > args.minsize:
                        file_key = filedigest + filesize
                        filepath = dir_entry.path
                        # print('Effing great file', filepath, threadName())
                        if file_key in paths:
                            # The key already has an entry, but the paths really really should be different!
                            if filepath == paths[file_key]:
                                print('Problem!', filepath, paths[file_key], dir_entry.is_symlink(), threadName())
                                exit(3)
                            new_dupe = Duplicate(filesize, filepath, paths[file_key])
                            # With folders we need to check for children so we can collapse a subtree. We have no such issue here
                            # because files hav no children. We do need later folder checks to recognize these file entries as children
                            # and remove them when dupe candidates are found which are parents of this file here. This should work since
                            # it's done by path, and files and folders use the same syntax
                            duplicates.append(new_dupe)
                        else:
                            paths[file_key] = filepath
                            sizes[file_key] = filesize

                else:
                    folder_digest, folder_size = searchTree(dir_entry.path)
                    digest += folder_digest
                    total_size += folder_size

        if total_size > args.minsize and digest != 0:
            with lock:
                folder_key = digest
                if folder_key in paths:
                    # If either path of a duplicate wholly contains either path of another duplicate, then it's a subtree
                    new_dupe = Duplicate(total_size, path, paths[folder_key])
                    orphan = True
                    children = {}
                    for already_registered_dupe in duplicates:
                        if new_dupe.isChildOf(already_registered_dupe):
                            # We have a parent, so our existence is pointless
                            # assert not children, 'We have a child and a parent. How did the child survive here without the parent already deleting it?'
                            # print('We have a parent')
                            orphan = False
                        if already_registered_dupe.isChildOf(new_dupe):
                            # We're the parent of an existing duplicate, so that child is now useless and should be removed
                            # assert orphan, 'We have a parent and a child. How did the child survive here without the parent already deleting it?'
                            # print('We have a child')
                            if already_registered_dupe.getKey() not in children:
                                children[already_registered_dupe.getKey()] = already_registered_dupe
                    if orphan:
                        duplicates.append(new_dupe)
                    for child in children.values():
                        duplicates.remove(child)
                else:
                    drive_threads = []
                    for th in threads.values():
                        if th.is_alive():
                            drive_threads.append(th.name)
                    paths[digest] = path
                    sizes[digest] = total_size
    except PermissionError:
        no_access.append(path)
    except FileNotFoundError:
        not_found.append(path)
    except NotADirectoryError:
        not_folder.append(path)

    return digest, total_size


drive_timings = []


def timedSearchTree(mounted_fs):
    threads[threading.get_ident()] = x
    start = time.time()
    searchTree(mounted_fs)
    end = time.time()
    drive_timings.append('Scanning drive %s took %d seconds' % (mounted_fs, end - start))


if __name__ == "__main__":

    for mounted_filesystem in mounted_filesystems.values():
        x = threading.Thread(target=timedSearchTree, args=(mounted_filesystem,), daemon=True, name=mounted_filesystem)
        x.start()

    for thread in threads.values():
        thread.join()

    sorted_duplicates = sorted(duplicates, key=operator.attrgetter('size'), reverse=True)
    print('Candidate duplicates:')
    for duplicate in sorted_duplicates:
        print(duplicate)
    print('----')
    for drive_timing in drive_timings:
        print(drive_timing)
    print('...done')
