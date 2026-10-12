'use strict';

const mongodb = require('mongodb');
const GridFSBucket = require('mongodb').GridFSBucket
const {createArchiveStream} = require('@patrickdk77/archive-defuse');
const {Readable} = require('stream');

function isPlainObject(value) {
  if (typeof value !== 'object' || value === null) return false

  if (Object.prototype.toString.call(value) !== '[object Object]') return false

  const proto = Object.getPrototypeOf(value);
  if (proto === null) return true

  const Ctor = Object.prototype.hasOwnProperty.call(proto, 'constructor') && proto.constructor;
  return (
      typeof Ctor === 'function' &&
      Ctor instanceof Ctor && Function.prototype.call(Ctor) === Function.prototype.call(value)
  );
}

function get(obj, path, defaultValue = undefined) {
  const travel = regexp =>
    String.prototype.split
      .call(path, regexp)
      .filter(Boolean)
      .reduce((res, key) => (res !== null && res !== undefined ? res[key] : res), obj);
  const result = travel(/[,[\]]+?/) || travel(/[,[\].]+?/);
  return result === undefined || result === obj ? defaultValue : result;
};

const ALLOWED_OPERATORS = new Set([
    'between', 'inq', 'nin', 'like', 'nlike', 'neq', 'regexp',
    'gt', 'gte', 'lt', 'lte', 'in', 'ne', 'exists', 'near', 'maxDistance', 'minDistance', 'geoWithin', 'nearSphere', 'geometry'
]);

function isSafePattern(pattern) {
    if (typeof pattern !== 'string') return true;
    if (pattern.length > 100) return false;
    // Check for nested quantifiers like (a+)+ or (a*)* or (\w+)* which cause catastrophic backtracking
    if (/\([^)]*[*+?{][^)]*\)[*+?{]/.test(pattern)) return false;
    // Check for adjacent repetition characters like ++ or *+
    if (/[*+?{][*+?}]/.test(pattern)) return false;
    return true;
}

function isSafeOptions(options) {
    if (options === null || options === undefined) return true;
    if (typeof options !== 'string') return false;
    if (options.length > 5) return false;
    return /^[gimsuy]*$/.test(options);
}

// archive-defuse refuses a zip entry name its own inspection would change, and a refusal ends the download partway,
// so every name goes through zipEntryName first. It follows archive-defuse's rules: no control or reordering
// characters, none of <>:"|?*\, no segment that is empty, "." or ".." or ends in a dot or space, no Windows device
// name, and nothing that leaves the folder once NFKC normalization or Windows best fit changes it.
const ZIP_CONTROL = /[\p{Cc}\u061c\u200e\u200f\u202a-\u202e\u2066-\u2069]/gu;
const ZIP_RESERVED = /[<>:"|?*\\]/g;
const ZIP_DEVICE = /^(con|prn|aux|nul|com[0-9\u00b9\u00b2\u00b3]|lpt[0-9\u00b9\u00b2\u00b3]|conin\$|conout\$)(\..*)?$/i;
const ZIP_DOTDOT = /^\.\.[. ]*$/;
// The characters Windows turns into a separator, a colon or a dot when it converts a name to the system code page.
const ZIP_BEST_FIT = {
    '\u00a5': '\\', '\u20a9': '\\', '\u2216': '\\', '\uff3c': '\\',
    '\u00b4': '/', '\u2044': '/', '\u2215': '/', '\uff0f': '/',
    '\u0589': ':', '\u2236': ':', '\uff1a': ':', '\uff0e': '.'
};
const ZIP_BEST_FIT_CHARS = /[\u00a5\u20a9\u2216\uff3c\u00b4\u2044\u2215\uff0f\u0589\u2236\uff1a\uff0e]/g;

// A loop, since a pattern such as /[. ]+$/ backtracks on long runs.
function trimEnd(s, chars) {
    let end = s.length;
    while (end > 0 && chars.includes(s[end - 1])) end--;
    return s.slice(0, end);
}

function cleanZipName(name) {
    const parts = name.replace(ZIP_CONTROL, '_').replace(ZIP_RESERVED, '_').split('/')
        .map(part => trimEnd(part, '. '))
        .filter(part => part !== '')
        .map(part => (ZIP_DEVICE.test(part) ? part.replace(/^([^.]*)/, '$1_') : part));
    return parts.join('/') || 'file';
}

function zipNameEscapes(name) {
    return /^[/\\]/.test(name) || /^[A-Za-z]:/.test(name) || name.split(/[/\\]/).some(part => ZIP_DOTDOT.test(part));
}

function zipEntryName(raw) {
    let name = cleanZipName(String(raw));
    for (let i = 0; i < 3; i++) {
        const folds = [name.normalize('NFKC'), name.replace(ZIP_BEST_FIT_CHARS, c => ZIP_BEST_FIT[c])];
        const escaping = folds.find(folded => folded !== name && zipNameEscapes(folded));
        if (escaping === undefined) return name;
        name = cleanZipName(escaping);
    }
    return cleanZipName(name.replace(/[^\x20-\x7e]/g, '_'));
}

module.exports = {
    statusError(message, status) {
        const error = new Error(message);
        error.status = status;
        return error;
    },
    replaceInPattern(pattern, obj) {
        return pattern.replace(/{\$([^}]+)}/g, (str, propPath) => {
            let res = str;
            const pathValue = get(obj, propPath);
            if (pathValue !== undefined) {
                res = pathValue;
            } else {
                console.warn(`Path: ${propPath} is undefined`);
            }
            return res;
        })
    },
    fileDownload(ctx, file, res, inline = false, namePattern = '{$filename}') {
        // set headers
        res.set('Content-Type', file.metadata.mimetype);
        res.set('Content-Length', file.length);
        res.set('Content-Disposition', `${(inline) ? 'inline' : 'attachment'};filename="${this.replaceInPattern(namePattern, file).normalize('NFD').replace(/[\u0300-\u036f]/g, '').replace(/[^\x20-\x7E]/g, '')}"; filename*=UTF-8''`+encodeURIComponent(this.replaceInPattern(namePattern, file)).replace(/['()*]/g,(c) => `%${c.charCodeAt(0).toString(16).toUpperCase()}`,).replace(/%(7C|60|5E)/g, (str, hex) => String.fromCharCode(parseInt(hex, 16)),));
        const gfs = new GridFSBucket(ctx.db);
        return gfs.openDownloadStream(file._id);
    },
    fileUpload(ctx, container, fileStream, filename, mimetype, customMetadata = {}) {
        const gfs = new GridFSBucket(ctx.db);
        const fileNameArr = filename.split('.');
        const metadata = Object.assign(customMetadata, {
            container,
            mimetype,
            extension: (fileNameArr.length > 1) ? fileNameArr.at(-1) : ''
        });
        const uploadStream = gfs.openUploadStream(filename, { metadata });
        return new Promise((resolve, reject) => {
            uploadStream.once('finish', (file) => {
                resolve(file);
            });
            uploadStream.on('error', (error) => {
                return reject(error);
            });
            fileStream.pipe(uploadStream);
        })
    },
    async fileUploadFromRequest(ctx, container, req) {
        let {files} = req;
        let fields = req.body || {};
        if (!files) {
            ({files, fields} = await this.parseFilesFromRequest(req));
        }
        const promises = files.map(({buffer, originalname, mimetype, fieldname}) => {
            const fileStream = new Readable({
                read() {
                    this.push(buffer);
                    this.push(null);
                }
            });
            let customMetadata = {};
            if (fieldname && fields[`${fieldname}_meta`]) {
                customMetadata = JSON.parse(fields[`${fieldname}_meta`]);
            }
            return this.fileUpload(ctx, container, fileStream, originalname, mimetype, customMetadata);
        });
        return Promise.all(promises);
    },
    zipDownload(ctx, files, res, zipName = 'files', namePattern = '{$filename}') {
        const gfs = new GridFSBucket(ctx.db);
        const entries = [];
        while (files.length) {
            const file = files.pop();
            // The file's stream opens when its turn comes, not all at once.
            entries.push({name: zipEntryName(this.replaceInPattern(namePattern, file)), data: () => gfs.openDownloadStream(file._id)});
        }
        const archive = createArchiveStream('zip', entries, {level: 9});
        // A response piped to stops reading when the client goes away, and the archive would wait for it forever.
        res.on('close', () => {
            if (!res.writableFinished) archive.destroy();
        });

        res.set('Content-Type', 'application/zip');
        res.set('Content-Disposition', `attachment;filename="${zipName.normalize('NFD').replace(/[\u0300-\u036f]/g, '').replace(/[^\x20-\x7E]/g, '')}.zip"; filename*=UTF-8''`+encodeURIComponent(`${zipName}.zip`).replace(/['()*]/g,(c) => `%${c.charCodeAt(0).toString(16).toUpperCase()}`,).replace(/%(7C|60|5E)/g, (str, hex) => String.fromCharCode(parseInt(hex, 16)),));
        return archive;
    },
    convertWhere(where) {
        const query = {};
        if (!isPlainObject(where)) {
            return query;
        }
        Object.keys(where).forEach((k) => {
            // Prevent Prototype Pollution
            if (k === '__proto__' || k === 'constructor' || k === 'prototype') {
                return;
            }
            let cond = where[k];
            if (['and', 'or', 'nor'].includes(k)) {
                if (Array.isArray(cond)) {
                    cond = cond.map((w) => {
                        return this.convertWhere(w);
                    });
                }
                query['$' + k] = cond;
                delete query[k];
                return;
            }
            if (k === 'id') {
                k = '_id';
            }
            let spec = '';
            let regexOptions = null;
            if (isPlainObject(cond)) {
                // Filter out any prototype properties in the sub-object
                const safeKeys = Object.keys(cond).filter(key => key !== '__proto__' && key !== 'constructor' && key !== 'prototype');
                if (safeKeys.length > 0) {
                    spec = safeKeys[0];
                    regexOptions = cond.options;
                    cond = cond[spec];
                } else {
                    return;
                }
            }
            if (spec) {
                // Enforce strict whitelist of allowed operators
                if (!ALLOWED_OPERATORS.has(spec)) {
                    return;
                }
                if (spec === 'between') {
                    query[k] = {$gte: this.typecastValue(k, cond[0]), $lte: this.typecastValue(k, cond[1])};
                } else if (spec === 'inq') {
                    cond = [].concat(cond || []);
                    query[k] = {
                        $in: cond.map((x) => {
                            return this.typecastValue(k, x);
                        }),
                    };
                } else if (spec === 'nin') {
                    cond = [].concat(cond || []);
                    query[k] = {
                        $nin: cond.map((x) => {
                            return this.typecastValue(k, x);
                        }),
                    };
                } else if (spec === 'like') {
                    if (cond instanceof RegExp) {
                        query[k] = {$regex: cond};
                    } else if (typeof cond === 'string' && isSafePattern(cond) && isSafeOptions(regexOptions)) {
                        query[k] = {$regex: cond, $options: regexOptions || ''};
                    }
                } else if (spec === 'nlike') {
                    if (cond instanceof RegExp) {
                        query[k] = {$not: cond};
                    } else if (typeof cond === 'string' && isSafePattern(cond) && isSafeOptions(regexOptions)) {
                        query[k] = {$not: {$regex: cond, $options: regexOptions || ''}};
                    }
                } else if (spec === 'neq') {
                    query[k] = {$ne: this.typecastValue(k, cond)};
                } else if (spec === 'regexp') {
                    if (cond instanceof RegExp) {
                        query[k] = {$regex: cond};
                    } else if (typeof cond === 'string' && isSafePattern(cond)) {
                        if (cond.global) {
                            console.warn('{{MongoDB}} regex syntax does not respect the {{`g`}} flag');
                        }
                        query[k] = {$regex: cond};
                    }
                } else {
                    query[k] = {};
                    query[k]['$' + spec] = this.typecastValue(k, cond);
                }
            } else {
                if (cond === null) {
                    // http://docs.mongodb.org/manual/reference/operator/query/type/
                    // Null: 10
                    query[k] = {$type: 10};
                } else {
                    query[k] = this.typecastValue(k, cond);
                }
            }
        });
        return query;
    },
    convertObjectId(id) {
        return mongodb.ObjectId ? new mongodb.ObjectId(id) : new mongodb.ObjectID(id)
    },
    typecastValue(key, val) {
        if (!this.props) {
            const raw = require('./models').file;
            this.props = {
                raw,
                objectIdFields: Object.keys(raw).filter(p => raw[p].type === 'ObjectID'),
                dateFields: Object.keys(raw).filter(p => raw[p].type === 'date')
            }
        }
        if (this.props.objectIdFields.includes(key))
            return this.convertObjectId(val);
        else if (this.props.dateFields.includes(key)) {
            return new Date(val);
        }
        return val;
    },
    // Parses a multipart/form-data body with Node's own parser, which holds the whole body in memory. A file input
    // left empty sends a part with no filename, which is skipped. A part with no type is text/plain, as busboy had it.
    async parseFilesFromRequest(req) {
        let form;
        try {
            form = await new Request('http://localhost/', {
                method: 'POST',
                headers: {'content-type': req.headers['content-type'] || ''},
                body: req,
                duplex: 'half'
            }).formData();
        } catch (error) {
            throw this.statusError(`The upload is not readable form data: ${error.message}`, 400);
        }
        const files = [];
        const fields = {};
        for (const [fieldname, value] of form) {
            if (typeof value === 'string') {
                fields[fieldname] = value;
            } else if (value.name) {
                files.push({
                    buffer: Buffer.from(await value.arrayBuffer()),
                    originalname: value.name,
                    mimetype: value.type || 'text/plain',
                    fieldname
                });
            }
        }
        return {files, fields};
    }
}