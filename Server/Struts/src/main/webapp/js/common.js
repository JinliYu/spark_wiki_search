!function() {
    "use strict";
    function a(b, d) {
        function e(a, b) {
            return function() {
                return a.apply(b, arguments)
            }
        }
        var f;
        if (d = d || {},
        this.trackingClick = !1,
        this.trackingClickStart = 0,
        this.targetElement = null ,
        this.touchStartX = 0,
        this.touchStartY = 0,
        this.lastTouchIdentifier = 0,
        this.touchBoundary = d.touchBoundary || 10,
        this.layer = b,
        this.tapDelay = d.tapDelay || 200,
        this.tapTimeout = d.tapTimeout || 700,
        !a.notNeeded(b)) {
            for (var g = ["onMouse", "onClick", "onTouchStart", "onTouchMove", "onTouchEnd", "onTouchCancel"], h = this, i = 0, j = g.length; j > i; i++)
                h[g[i]] = e(h[g[i]], h);
            c && (b.addEventListener("mouseover", this.onMouse, !0),
            b.addEventListener("mousedown", this.onMouse, !0),
            b.addEventListener("mouseup", this.onMouse, !0)),
            b.addEventListener("click", this.onClick, !0),
            b.addEventListener("touchstart", this.onTouchStart, !1),
            b.addEventListener("touchmove", this.onTouchMove, !1),
            b.addEventListener("touchend", this.onTouchEnd, !1),
            b.addEventListener("touchcancel", this.onTouchCancel, !1),
            Event.prototype.stopImmediatePropagation || (b.removeEventListener = function(a, c, d) {
                var e = Node.prototype.removeEventListener;
                "click" === a ? e.call(b, a, c.hijacked || c, d) : e.call(b, a, c, d)
            }
            ,
            b.addEventListener = function(a, c, d) {
                var e = Node.prototype.addEventListener;
                "click" === a ? e.call(b, a, c.hijacked || (c.hijacked = function(a) {
                    a.propagationStopped || c(a)
                }
                ), d) : e.call(b, a, c, d)
            }
            ),
            "function" == typeof b.onclick && (f = b.onclick,
            b.addEventListener("click", function(a) {
                f(a)
            }, !1),
            b.onclick = null )
        }
    }
    var b = navigator.userAgent.indexOf("Windows Phone") >= 0
      , c = navigator.userAgent.indexOf("Android") > 0 && !b
      , d = /iP(ad|hone|od)/.test(navigator.userAgent) && !b
      , e = d && /OS 4_\d(_\d)?/.test(navigator.userAgent)
      , f = d && /OS [6-7]_\d/.test(navigator.userAgent)
      , g = navigator.userAgent.indexOf("BB10") > 0;
    a.prototype.needsClick = function(a) {
        switch (a.nodeName.toLowerCase()) {
        case "button":
        case "select":
        case "textarea":
            if (a.disabled)
                return !0;
            break;
        case "input":
            if (d && "file" === a.type || a.disabled)
                return !0;
            break;
        case "label":
        case "iframe":
        case "video":
            return !0
        }
        return /\bneedsclick\b/.test(a.className)
    }
    ,
    a.prototype.needsFocus = function(a) {
        switch (a.nodeName.toLowerCase()) {
        case "textarea":
            return !0;
        case "select":
            return !c;
        case "input":
            switch (a.type) {
            case "button":
            case "checkbox":
            case "file":
            case "image":
            case "radio":
            case "submit":
                return !1
            }
            return !a.disabled && !a.readOnly;
        default:
            return /\bneedsfocus\b/.test(a.className)
        }
    }
    ,
    a.prototype.sendClick = function(a, b) {
        var c, d;
        document.activeElement && document.activeElement !== a && document.activeElement.blur(),
        d = b.changedTouches[0],
        c = document.createEvent("MouseEvents"),
        c.initMouseEvent(this.determineEventType(a), !0, !0, window, 1, d.screenX, d.screenY, d.clientX, d.clientY, !1, !1, !1, !1, 0, null ),
        c.forwardedTouchEvent = !0,
        a.dispatchEvent(c)
    }
    ,
    a.prototype.determineEventType = function(a) {
        return c && "select" === a.tagName.toLowerCase() ? "mousedown" : "click"
    }
    ,
    a.prototype.focus = function(a) {
        var b;
        d && a.setSelectionRange && 0 !== a.type.indexOf("date") && "time" !== a.type && "month" !== a.type ? (b = a.value.length,
        a.setSelectionRange(b, b)) : a.focus()
    }
    ,
    a.prototype.updateScrollParent = function(a) {
        var b, c;
        if (b = a.fastClickScrollParent,
        !b || !b.contains(a)) {
            c = a;
            do {
                if (c.scrollHeight > c.offsetHeight) {
                    b = c,
                    a.fastClickScrollParent = c;
                    break
                }
                c = c.parentElement
            } while (c)
        }
        b && (b.fastClickLastScrollTop = b.scrollTop)
    }
    ,
    a.prototype.getTargetElementFromEventTarget = function(a) {
        return a.nodeType === Node.TEXT_NODE ? a.parentNode : a
    }
    ,
    a.prototype.onTouchStart = function(a) {
        var b, c, f;
        if (a.targetTouches.length > 1)
            return !0;
        if (b = this.getTargetElementFromEventTarget(a.target),
        c = a.targetTouches[0],
        d) {
            if (f = window.getSelection(),
            f.rangeCount && !f.isCollapsed)
                return !0;
            if (!e) {
                if (c.identifier && c.identifier === this.lastTouchIdentifier)
                    return a.preventDefault(),
                    !1;
                this.lastTouchIdentifier = c.identifier,
                this.updateScrollParent(b)
            }
        }
        return this.trackingClick = !0,
        this.trackingClickStart = a.timeStamp,
        this.targetElement = b,
        this.touchStartX = c.pageX,
        this.touchStartY = c.pageY,
        a.timeStamp - this.lastClickTime < this.tapDelay && a.preventDefault(),
        !0
    }
    ,
    a.prototype.touchHasMoved = function(a) {
        var b = a.changedTouches[0]
          , c = this.touchBoundary;
        return Math.abs(b.pageX - this.touchStartX) > c || Math.abs(b.pageY - this.touchStartY) > c
    }
    ,
    a.prototype.onTouchMove = function(a) {
        return this.trackingClick ? ((this.targetElement !== this.getTargetElementFromEventTarget(a.target) || this.touchHasMoved(a)) && (this.trackingClick = !1,
        this.targetElement = null ),
        !0) : !0
    }
    ,
    a.prototype.findControl = function(a) {
        return void 0 !== a.control ? a.control : a.htmlFor ? document.getElementById(a.htmlFor) : a.querySelector("button, input:not([type=hidden]), keygen, meter, output, progress, select, textarea")
    }
    ,
    a.prototype.onTouchEnd = function(a) {
        var b, g, h, i, j, k = this.targetElement;
        if (!this.trackingClick)
            return !0;
        if (a.timeStamp - this.lastClickTime < this.tapDelay)
            return this.cancelNextClick = !0,
            !0;
        if (a.timeStamp - this.trackingClickStart > this.tapTimeout)
            return !0;
        if (this.cancelNextClick = !1,
        this.lastClickTime = a.timeStamp,
        g = this.trackingClickStart,
        this.trackingClick = !1,
        this.trackingClickStart = 0,
        f && (j = a.changedTouches[0],
        k = document.elementFromPoint(j.pageX - window.pageXOffset, j.pageY - window.pageYOffset) || k,
        k.fastClickScrollParent = this.targetElement.fastClickScrollParent),
        h = k.tagName.toLowerCase(),
        "label" === h) {
            if (b = this.findControl(k)) {
                if (this.focus(k),
                c)
                    return !1;
                k = b
            }
        } else if (this.needsFocus(k))
            return a.timeStamp - g > 100 || d && window.top !== window && "input" === h ? (this.targetElement = null ,
            !1) : (this.focus(k),
            this.sendClick(k, a),
            d && "select" === h || (this.targetElement = null ,
            a.preventDefault()),
            !1);
        return d && !e && (i = k.fastClickScrollParent,
        i && i.fastClickLastScrollTop !== i.scrollTop) ? !0 : (this.needsClick(k) || (a.preventDefault(),
        this.sendClick(k, a)),
        !1)
    }
    ,
    a.prototype.onTouchCancel = function() {
        this.trackingClick = !1,
        this.targetElement = null 
    }
    ,
    a.prototype.onMouse = function(a) {
        return this.targetElement ? a.forwardedTouchEvent ? !0 : a.cancelable && (!this.needsClick(this.targetElement) || this.cancelNextClick) ? (a.stopImmediatePropagation ? a.stopImmediatePropagation() : a.propagationStopped = !0,
        a.stopPropagation(),
        a.preventDefault(),
        !1) : !0 : !0
    }
    ,
    a.prototype.onClick = function(a) {
        var b;
        return this.trackingClick ? (this.targetElement = null ,
        this.trackingClick = !1,
        !0) : "submit" === a.target.type && 0 === a.detail ? !0 : (b = this.onMouse(a),
        b || (this.targetElement = null ),
        b)
    }
    ,
    a.prototype.destroy = function() {
        var a = this.layer;
        c && (a.removeEventListener("mouseover", this.onMouse, !0),
        a.removeEventListener("mousedown", this.onMouse, !0),
        a.removeEventListener("mouseup", this.onMouse, !0)),
        a.removeEventListener("click", this.onClick, !0),
        a.removeEventListener("touchstart", this.onTouchStart, !1),
        a.removeEventListener("touchmove", this.onTouchMove, !1),
        a.removeEventListener("touchend", this.onTouchEnd, !1),
        a.removeEventListener("touchcancel", this.onTouchCancel, !1)
    }
    ,
    a.notNeeded = function(a) {
        var b, d, e, f;
        if ("undefined" == typeof window.ontouchstart)
            return !0;
        if (d = +(/Chrome\/([0-9]+)/.exec(navigator.userAgent) || [, 0])[1]) {
            if (!c)
                return !0;
            if (b = document.querySelector("meta[name=viewport]")) {
                if (-1 !== b.content.indexOf("user-scalable=no"))
                    return !0;
                if (d > 31 && document.documentElement.scrollWidth <= window.outerWidth)
                    return !0
            }
        }
        if (g && (e = navigator.userAgent.match(/Version\/([0-9]*)\.([0-9]*)/),
        e[1] >= 10 && e[2] >= 3 && (b = document.querySelector("meta[name=viewport]")))) {
            if (-1 !== b.content.indexOf("user-scalable=no"))
                return !0;
            if (document.documentElement.scrollWidth <= window.outerWidth)
                return !0
        }
        return "none" === a.style.msTouchAction || "manipulation" === a.style.touchAction ? !0 : (f = +(/Firefox\/([0-9]+)/.exec(navigator.userAgent) || [, 0])[1],
        f >= 27 && (b = document.querySelector("meta[name=viewport]"),
        b && (-1 !== b.content.indexOf("user-scalable=no") || document.documentElement.scrollWidth <= window.outerWidth)) ? !0 : "none" === a.style.touchAction || "manipulation" === a.style.touchAction)
    }
    ,
    a.attach = function(b, c) {
        return new a(b,c)
    }
    ,
    "function" == typeof define && "object" == typeof define.amd && define.amd ? define(function() {
        return a
    }) : "undefined" != typeof module && module.exports ? (module.exports = a.attach,
    module.exports.FastClick = a) : window.FastClick = a
}(),
$(function() {
    FastClick.attach(document.body)
}),
function() {
    var a = this
      , b = a._
      , c = {}
      , d = Array.prototype
      , e = Object.prototype
      , f = Function.prototype
      , g = d.push
      , h = d.slice
      , i = d.concat
      , j = e.toString
      , k = e.hasOwnProperty
      , l = d.forEach
      , m = d.map
      , n = d.reduce
      , o = d.reduceRight
      , p = d.filter
      , q = d.every
      , r = d.some
      , s = d.indexOf
      , t = d.lastIndexOf
      , u = Array.isArray
      , v = Object.keys
      , w = f.bind
      , x = function(a) {
        return a instanceof x ? a : this instanceof x ? void (this._wrapped = a) : new x(a)
    }
    ;
    "undefined" != typeof exports ? ("undefined" != typeof module && module.exports && (exports = module.exports = x),
    exports._ = x) : a._ = x,
    x.VERSION = "1.5.2";
    var y = x.each = x.forEach = function(a, b, d) {
        if (null  != a)
            if (l && a.forEach === l)
                a.forEach(b, d);
            else if (a.length === +a.length) {
                for (var e = 0, f = a.length; f > e; e++)
                    if (b.call(d, a[e], e, a) === c)
                        return
            } else
                for (var g = x.keys(a), e = 0, f = g.length; f > e; e++)
                    if (b.call(d, a[g[e]], g[e], a) === c)
                        return
    }
    ;
    x.map = x.collect = function(a, b, c) {
        var d = [];
        return null  == a ? d : m && a.map === m ? a.map(b, c) : (y(a, function(a, e, f) {
            d.push(b.call(c, a, e, f))
        }),
        d)
    }
    ;
    var z = "Reduce of empty array with no initial value";
    x.reduce = x.foldl = x.inject = function(a, b, c, d) {
        var e = arguments.length > 2;
        if (null  == a && (a = []),
        n && a.reduce === n)
            return d && (b = x.bind(b, d)),
            e ? a.reduce(b, c) : a.reduce(b);
        if (y(a, function(a, f, g) {
            e ? c = b.call(d, c, a, f, g) : (c = a,
            e = !0)
        }),
        !e)
            throw new TypeError(z);
        return c
    }
    ,
    x.reduceRight = x.foldr = function(a, b, c, d) {
        var e = arguments.length > 2;
        if (null  == a && (a = []),
        o && a.reduceRight === o)
            return d && (b = x.bind(b, d)),
            e ? a.reduceRight(b, c) : a.reduceRight(b);
        var f = a.length;
        if (f !== +f) {
            var g = x.keys(a);
            f = g.length
        }
        if (y(a, function(h, i, j) {
            i = g ? g[--f] : --f,
            e ? c = b.call(d, c, a[i], i, j) : (c = a[i],
            e = !0)
        }),
        !e)
            throw new TypeError(z);
        return c
    }
    ,
    x.find = x.detect = function(a, b, c) {
        var d;
        return A(a, function(a, e, f) {
            return b.call(c, a, e, f) ? (d = a,
            !0) : void 0
        }),
        d
    }
    ,
    x.filter = x.select = function(a, b, c) {
        var d = [];
        return null  == a ? d : p && a.filter === p ? a.filter(b, c) : (y(a, function(a, e, f) {
            b.call(c, a, e, f) && d.push(a)
        }),
        d)
    }
    ,
    x.reject = function(a, b, c) {
        return x.filter(a, function(a, d, e) {
            return !b.call(c, a, d, e)
        }, c)
    }
    ,
    x.every = x.all = function(a, b, d) {
        b || (b = x.identity);
        var e = !0;
        return null  == a ? e : q && a.every === q ? a.every(b, d) : (y(a, function(a, f, g) {
            return (e = e && b.call(d, a, f, g)) ? void 0 : c
        }),
        !!e)
    }
    ;
    var A = x.some = x.any = function(a, b, d) {
        b || (b = x.identity);
        var e = !1;
        return null  == a ? e : r && a.some === r ? a.some(b, d) : (y(a, function(a, f, g) {
            return e || (e = b.call(d, a, f, g)) ? c : void 0
        }),
        !!e)
    }
    ;
    x.contains = x.include = function(a, b) {
        return null  == a ? !1 : s && a.indexOf === s ? -1 != a.indexOf(b) : A(a, function(a) {
            return a === b
        })
    }
    ,
    x.invoke = function(a, b) {
        var c = h.call(arguments, 2)
          , d = x.isFunction(b);
        return x.map(a, function(a) {
            return (d ? b : a[b]).apply(a, c)
        })
    }
    ,
    x.pluck = function(a, b) {
        return x.map(a, function(a) {
            return a[b]
        })
    }
    ,
    x.where = function(a, b, c) {
        return x.isEmpty(b) ? c ? void 0 : [] : x[c ? "find" : "filter"](a, function(a) {
            for (var c in b)
                if (b[c] !== a[c])
                    return !1;
            return !0
        })
    }
    ,
    x.findWhere = function(a, b) {
        return x.where(a, b, !0)
    }
    ,
    x.max = function(a, b, c) {
        if (!b && x.isArray(a) && a[0] === +a[0] && a.length < 65535)
            return Math.max.apply(Math, a);
        if (!b && x.isEmpty(a))
            return -(1 / 0);
        var d = {
            computed: -(1 / 0),
            value: -(1 / 0)
        };
        return y(a, function(a, e, f) {
            var g = b ? b.call(c, a, e, f) : a;
            g > d.computed && (d = {
                value: a,
                computed: g
            })
        }),
        d.value
    }
    ,
    x.min = function(a, b, c) {
        if (!b && x.isArray(a) && a[0] === +a[0] && a.length < 65535)
            return Math.min.apply(Math, a);
        if (!b && x.isEmpty(a))
            return 1 / 0;
        var d = {
            computed: 1 / 0,
            value: 1 / 0
        };
        return y(a, function(a, e, f) {
            var g = b ? b.call(c, a, e, f) : a;
            g < d.computed && (d = {
                value: a,
                computed: g
            })
        }),
        d.value
    }
    ,
    x.shuffle = function(a) {
        var b, c = 0, d = [];
        return y(a, function(a) {
            b = x.random(c++),
            d[c - 1] = d[b],
            d[b] = a
        }),
        d
    }
    ,
    x.sample = function(a, b, c) {
        return arguments.length < 2 || c ? a[x.random(a.length - 1)] : x.shuffle(a).slice(0, Math.max(0, b))
    }
    ;
    var B = function(a) {
        return x.isFunction(a) ? a : function(b) {
            return b[a]
        }
    }
    ;
    x.sortBy = function(a, b, c) {
        var d = B(b);
        return x.pluck(x.map(a, function(a, b, e) {
            return {
                value: a,
                index: b,
                criteria: d.call(c, a, b, e)
            }
        }).sort(function(a, b) {
            var c = a.criteria
              , d = b.criteria;
            if (c !== d) {
                if (c > d || void 0 === c)
                    return 1;
                if (d > c || void 0 === d)
                    return -1
            }
            return a.index - b.index
        }), "value")
    }
    ;
    var C = function(a) {
        return function(b, c, d) {
            var e = {}
              , f = null  == c ? x.identity : B(c);
            return y(b, function(c, g) {
                var h = f.call(d, c, g, b);
                a(e, h, c)
            }),
            e
        }
    }
    ;
    x.groupBy = C(function(a, b, c) {
        (x.has(a, b) ? a[b] : a[b] = []).push(c)
    }),
    x.indexBy = C(function(a, b, c) {
        a[b] = c
    }),
    x.countBy = C(function(a, b) {
        x.has(a, b) ? a[b]++ : a[b] = 1
    }),
    x.sortedIndex = function(a, b, c, d) {
        c = null  == c ? x.identity : B(c);
        for (var e = c.call(d, b), f = 0, g = a.length; g > f; ) {
            var h = f + g >>> 1;
            c.call(d, a[h]) < e ? f = h + 1 : g = h
        }
        return f
    }
    ,
    x.toArray = function(a) {
        return a ? x.isArray(a) ? h.call(a) : a.length === +a.length ? x.map(a, x.identity) : x.values(a) : []
    }
    ,
    x.size = function(a) {
        return null  == a ? 0 : a.length === +a.length ? a.length : x.keys(a).length
    }
    ,
    x.first = x.head = x.take = function(a, b, c) {
        return null  != a ? null  == b || c ? a[0] : h.call(a, 0, b) : void 0
    }
    ,
    x.initial = function(a, b, c) {
        return h.call(a, 0, a.length - (null  == b || c ? 1 : b))
    }
    ,
    x.last = function(a, b, c) {
        return null  != a ? null  == b || c ? a[a.length - 1] : h.call(a, Math.max(a.length - b, 0)) : void 0
    }
    ,
    x.rest = x.tail = x.drop = function(a, b, c) {
        return h.call(a, null  == b || c ? 1 : b)
    }
    ,
    x.compact = function(a) {
        return x.filter(a, x.identity)
    }
    ;
    var D = function(a, b, c) {
        return b && x.every(a, x.isArray) ? i.apply(c, a) : (y(a, function(a) {
            x.isArray(a) || x.isArguments(a) ? b ? g.apply(c, a) : D(a, b, c) : c.push(a)
        }),
        c)
    }
    ;
    x.flatten = function(a, b) {
        return D(a, b, [])
    }
    ,
    x.without = function(a) {
        return x.difference(a, h.call(arguments, 1))
    }
    ,
    x.uniq = x.unique = function(a, b, c, d) {
        x.isFunction(b) && (d = c,
        c = b,
        b = !1);
        var e = c ? x.map(a, c, d) : a
          , f = []
          , g = [];
        return y(e, function(c, d) {
            (b ? d && g[g.length - 1] === c : x.contains(g, c)) || (g.push(c),
            f.push(a[d]))
        }),
        f
    }
    ,
    x.union = function() {
        return x.uniq(x.flatten(arguments, !0))
    }
    ,
    x.intersection = function(a) {
        var b = h.call(arguments, 1);
        return x.filter(x.uniq(a), function(a) {
            return x.every(b, function(b) {
                return x.indexOf(b, a) >= 0
            })
        })
    }
    ,
    x.difference = function(a) {
        var b = i.apply(d, h.call(arguments, 1));
        return x.filter(a, function(a) {
            return !x.contains(b, a)
        })
    }
    ,
    x.zip = function() {
        for (var a = x.max(x.pluck(arguments, "length").concat(0)), b = new Array(a), c = 0; a > c; c++)
            b[c] = x.pluck(arguments, "" + c);
        return b
    }
    ,
    x.object = function(a, b) {
        if (null  == a)
            return {};
        for (var c = {}, d = 0, e = a.length; e > d; d++)
            b ? c[a[d]] = b[d] : c[a[d][0]] = a[d][1];
        return c
    }
    ,
    x.indexOf = function(a, b, c) {
        if (null  == a)
            return -1;
        var d = 0
          , e = a.length;
        if (c) {
            if ("number" != typeof c)
                return d = x.sortedIndex(a, b),
                a[d] === b ? d : -1;
            d = 0 > c ? Math.max(0, e + c) : c
        }
        if (s && a.indexOf === s)
            return a.indexOf(b, c);
        for (; e > d; d++)
            if (a[d] === b)
                return d;
        return -1
    }
    ,
    x.lastIndexOf = function(a, b, c) {
        if (null  == a)
            return -1;
        var d = null  != c;
        if (t && a.lastIndexOf === t)
            return d ? a.lastIndexOf(b, c) : a.lastIndexOf(b);
        for (var e = d ? c : a.length; e--; )
            if (a[e] === b)
                return e;
        return -1
    }
    ,
    x.range = function(a, b, c) {
        arguments.length <= 1 && (b = a || 0,
        a = 0),
        c = arguments[2] || 1;
        for (var d = Math.max(Math.ceil((b - a) / c), 0), e = 0, f = new Array(d); d > e; )
            f[e++] = a,
            a += c;
        return f
    }
    ;
    var E = function() {}
    ;
    x.bind = function(a, b) {
        var c, d;
        if (w && a.bind === w)
            return w.apply(a, h.call(arguments, 1));
        if (!x.isFunction(a))
            throw new TypeError;
        return c = h.call(arguments, 2),
        d = function() {
            if (!(this instanceof d))
                return a.apply(b, c.concat(h.call(arguments)));
            E.prototype = a.prototype;
            var e = new E;
            E.prototype = null ;
            var f = a.apply(e, c.concat(h.call(arguments)));
            return Object(f) === f ? f : e
        }
    }
    ,
    x.partial = function(a) {
        var b = h.call(arguments, 1);
        return function() {
            return a.apply(this, b.concat(h.call(arguments)))
        }
    }
    ,
    x.bindAll = function(a) {
        var b = h.call(arguments, 1);
        if (0 === b.length)
            throw new Error("bindAll must be passed function names");
        return y(b, function(b) {
            a[b] = x.bind(a[b], a)
        }),
        a
    }
    ,
    x.memoize = function(a, b) {
        var c = {};
        return b || (b = x.identity),
        function() {
            var d = b.apply(this, arguments);
            return x.has(c, d) ? c[d] : c[d] = a.apply(this, arguments)
        }
    }
    ,
    x.delay = function(a, b) {
        var c = h.call(arguments, 2);
        return setTimeout(function() {
            return a.apply(null , c)
        }, b)
    }
    ,
    x.defer = function(a) {
        return x.delay.apply(x, [a, 1].concat(h.call(arguments, 1)))
    }
    ,
    x.throttle = function(a, b, c) {
        var d, e, f, g = null , h = 0;
        c || (c = {});
        var i = function() {
            h = c.leading === !1 ? 0 : new Date,
            g = null ,
            f = a.apply(d, e)
        }
        ;
        return function() {
            var j = new Date;
            h || c.leading !== !1 || (h = j);
            var k = b - (j - h);
            return d = this,
            e = arguments,
            0 >= k ? (clearTimeout(g),
            g = null ,
            h = j,
            f = a.apply(d, e)) : g || c.trailing === !1 || (g = setTimeout(i, k)),
            f
        }
    }
    ,
    x.debounce = function(a, b, c) {
        var d, e, f, g, h;
        return function() {
            f = this,
            e = arguments,
            g = new Date;
            var i = function() {
                var j = new Date - g;
                b > j ? d = setTimeout(i, b - j) : (d = null ,
                c || (h = a.apply(f, e)))
            }
              , j = c && !d;
            return d || (d = setTimeout(i, b)),
            j && (h = a.apply(f, e)),
            h
        }
    }
    ,
    x.once = function(a) {
        var b, c = !1;
        return function() {
            return c ? b : (c = !0,
            b = a.apply(this, arguments),
            a = null ,
            b)
        }
    }
    ,
    x.wrap = function(a, b) {
        return function() {
            var c = [a];
            return g.apply(c, arguments),
            b.apply(this, c)
        }
    }
    ,
    x.compose = function() {
        var a = arguments;
        return function() {
            for (var b = arguments, c = a.length - 1; c >= 0; c--)
                b = [a[c].apply(this, b)];
            return b[0]
        }
    }
    ,
    x.after = function(a, b) {
        return function() {
            return --a < 1 ? b.apply(this, arguments) : void 0
        }
    }
    ,
    x.keys = v || function(a) {
        if (a !== Object(a))
            throw new TypeError("Invalid object");
        var b = [];
        for (var c in a)
            x.has(a, c) && b.push(c);
        return b
    }
    ,
    x.values = function(a) {
        for (var b = x.keys(a), c = b.length, d = new Array(c), e = 0; c > e; e++)
            d[e] = a[b[e]];
        return d
    }
    ,
    x.pairs = function(a) {
        for (var b = x.keys(a), c = b.length, d = new Array(c), e = 0; c > e; e++)
            d[e] = [b[e], a[b[e]]];
        return d
    }
    ,
    x.invert = function(a) {
        for (var b = {}, c = x.keys(a), d = 0, e = c.length; e > d; d++)
            b[a[c[d]]] = c[d];
        return b
    }
    ,
    x.functions = x.methods = function(a) {
        var b = [];
        for (var c in a)
            x.isFunction(a[c]) && b.push(c);
        return b.sort()
    }
    ,
    x.extend = function(a) {
        return y(h.call(arguments, 1), function(b) {
            if (b)
                for (var c in b)
                    a[c] = b[c]
        }),
        a
    }
    ,
    x.pick = function(a) {
        var b = {}
          , c = i.apply(d, h.call(arguments, 1));
        return y(c, function(c) {
            c in a && (b[c] = a[c])
        }),
        b
    }
    ,
    x.omit = function(a) {
        var b = {}
          , c = i.apply(d, h.call(arguments, 1));
        for (var e in a)
            x.contains(c, e) || (b[e] = a[e]);
        return b
    }
    ,
    x.defaults = function(a) {
        return y(h.call(arguments, 1), function(b) {
            if (b)
                for (var c in b)
                    void 0 === a[c] && (a[c] = b[c])
        }),
        a
    }
    ,
    x.clone = function(a) {
        return x.isObject(a) ? x.isArray(a) ? a.slice() : x.extend({}, a) : a
    }
    ,
    x.tap = function(a, b) {
        return b(a),
        a
    }
    ;
    var F = function(a, b, c, d) {
        if (a === b)
            return 0 !== a || 1 / a == 1 / b;
        if (null  == a || null  == b)
            return a === b;
        a instanceof x && (a = a._wrapped),
        b instanceof x && (b = b._wrapped);
        var e = j.call(a);
        if (e != j.call(b))
            return !1;
        switch (e) {
        case "[object String]":
            return a == String(b);
        case "[object Number]":
            return a != +a ? b != +b : 0 == a ? 1 / a == 1 / b : a == +b;
        case "[object Date]":
        case "[object Boolean]":
            return +a == +b;
        case "[object RegExp]":
            return a.source == b.source && a.global == b.global && a.multiline == b.multiline && a.ignoreCase == b.ignoreCase
        }
        if ("object" != typeof a || "object" != typeof b)
            return !1;
        for (var f = c.length; f--; )
            if (c[f] == a)
                return d[f] == b;
        var g = a.constructor
          , h = b.constructor;
        if (g !== h && !(x.isFunction(g) && g instanceof g && x.isFunction(h) && h instanceof h))
            return !1;
        c.push(a),
        d.push(b);
        var i = 0
          , k = !0;
        if ("[object Array]" == e) {
            if (i = a.length,
            k = i == b.length)
                for (; i-- && (k = F(a[i], b[i], c, d)); )
                    ;
        } else {
            for (var l in a)
                if (x.has(a, l) && (i++,
                !(k = x.has(b, l) && F(a[l], b[l], c, d))))
                    break;
            if (k) {
                for (l in b)
                    if (x.has(b, l) && !i--)
                        break;
                k = !i
            }
        }
        return c.pop(),
        d.pop(),
        k
    }
    ;
    x.isEqual = function(a, b) {
        return F(a, b, [], [])
    }
    ,
    x.isEmpty = function(a) {
        if (null  == a)
            return !0;
        if (x.isArray(a) || x.isString(a))
            return 0 === a.length;
        for (var b in a)
            if (x.has(a, b))
                return !1;
        return !0
    }
    ,
    x.isElement = function(a) {
        return !(!a || 1 !== a.nodeType)
    }
    ,
    x.isArray = u || function(a) {
        return "[object Array]" == j.call(a)
    }
    ,
    x.isObject = function(a) {
        return a === Object(a)
    }
    ,
    y(["Arguments", "Function", "String", "Number", "Date", "RegExp"], function(a) {
        x["is" + a] = function(b) {
            return j.call(b) == "[object " + a + "]"
        }
    }),
    x.isArguments(arguments) || (x.isArguments = function(a) {
        return !(!a || !x.has(a, "callee"))
    }
    ),
    "function" != typeof /./ && (x.isFunction = function(a) {
        return "function" == typeof a
    }
    ),
    x.isFinite = function(a) {
        return isFinite(a) && !isNaN(parseFloat(a))
    }
    ,
    x.isNaN = function(a) {
        return x.isNumber(a) && a != +a
    }
    ,
    x.isBoolean = function(a) {
        return a === !0 || a === !1 || "[object Boolean]" == j.call(a)
    }
    ,
    x.isNull = function(a) {
        return null  === a
    }
    ,
    x.isUndefined = function(a) {
        return void 0 === a
    }
    ,
    x.has = function(a, b) {
        return k.call(a, b)
    }
    ,
    x.noConflict = function() {
        return a._ = b,
        this
    }
    ,
    x.identity = function(a) {
        return a
    }
    ,
    x.times = function(a, b, c) {
        for (var d = Array(Math.max(0, a)), e = 0; a > e; e++)
            d[e] = b.call(c, e);
        return d
    }
    ,
    x.random = function(a, b) {
        return null  == b && (b = a,
        a = 0),
        a + Math.floor(Math.random() * (b - a + 1))
    }
    ;
    var G = {
        escape: {
            "&": "&amp;",
            "<": "&lt;",
            ">": "&gt;",
            '"': "&quot;",
            "'": "&#x27;"
        }
    };
    G.unescape = x.invert(G.escape);
    var H = {
        escape: new RegExp("[" + x.keys(G.escape).join("") + "]","g"),
        unescape: new RegExp("(" + x.keys(G.unescape).join("|") + ")","g")
    };
    x.each(["escape", "unescape"], function(a) {
        x[a] = function(b) {
            return null  == b ? "" : ("" + b).replace(H[a], function(b) {
                return G[a][b]
            })
        }
    }),
    x.result = function(a, b) {
        if (null  != a) {
            var c = a[b];
            return x.isFunction(c) ? c.call(a) : c
        }
    }
    ,
    x.mixin = function(a) {
        y(x.functions(a), function(b) {
            var c = x[b] = a[b];
            x.prototype[b] = function() {
                var a = [this._wrapped];
                return g.apply(a, arguments),
                M.call(this, c.apply(x, a))
            }
        })
    }
    ;
    var I = 0;
    x.uniqueId = function(a) {
        var b = ++I + "";
        return a ? a + b : b
    }
    ,
    x.templateSettings = {
        evaluate: /<%([\s\S]+?)%>/g,
        interpolate: /<%=([\s\S]+?)%>/g,
        escape: /<%-([\s\S]+?)%>/g
    };
    var J = /(.)^/
      , K = {
        "'": "'",
        "\\": "\\",
        "\r": "r",
        "\n": "n",
        "   ": "t",
        "\u2028": "u2028",
        "\u2029": "u2029"
    }
      , L = /\\|'|\r|\n|\t|\u2028|\u2029/g;
    x.template = function(a, b, c) {
        var d;
        c = x.defaults({}, c, x.templateSettings);
        var e = new RegExp([(c.escape || J).source, (c.interpolate || J).source, (c.evaluate || J).source].join("|") + "|$","g")
          , f = 0
          , g = "__p+='";
        a.replace(e, function(b, c, d, e, h) {
            return g += a.slice(f, h).replace(L, function(a) {
                return "\\" + K[a]
            }),
            c && (g += "'+\n((__t=(" + c + "))==null?'':_.escape(__t))+\n'"),
            d && (g += "'+\n((__t=(" + d + "))==null?'':__t)+\n'"),
            e && (g += "';\n" + e + "\n__p+='"),
            f = h + b.length,
            b
        }),
        g += "';\n",
        c.variable || (g = "with(obj||{}){\n" + g + "}\n"),
        g = "var __t,__p='',__j=Array.prototype.join,print=function(){__p+=__j.call(arguments,'');};\n" + g + "return __p;\n";
        try {
            d = new Function(c.variable || "obj","_",g)
        } catch (h) {
            throw h.source = g,
            h
        }
        if (b)
            return d(b, x);
        var i = function(a) {
            return d.call(this, a, x)
        }
        ;
        return i.source = "function(" + (c.variable || "obj") + "){\n" + g + "}",
        i
    }
    ,
    x.chain = function(a) {
        return x(a).chain()
    }
    ;
    var M = function(a) {
        return this._chain ? x(a).chain() : a
    }
    ;
    x.mixin(x),
    y(["pop", "push", "reverse", "shift", "sort", "splice", "unshift"], function(a) {
        var b = d[a];
        x.prototype[a] = function() {
            var c = this._wrapped;
            return b.apply(c, arguments),
            "shift" != a && "splice" != a || 0 !== c.length || delete c[0],
            M.call(this, c)
        }
    }),
    y(["concat", "join", "slice"], function(a) {
        var b = d[a];
        x.prototype[a] = function() {
            return M.call(this, b.apply(this._wrapped, arguments))
        }
    }),
    x.extend(x.prototype, {
        chain: function() {
            return this._chain = !0,
            this
        },
        value: function() {
            return this._wrapped
        }
    })
}
.call(this),
!function(a, b, c) {
    function d(a, c) {
        var d, e = b.createElement(a || "div");
        for (d in c)
            e[d] = c[d];
        return e
    }
    function e(a) {
        for (var b = 1, c = arguments.length; c > b; b++)
            a.appendChild(arguments[b]);
        return a
    }
    function f(a, b, c, d) {
        var e = ["opacity", b, ~~(100 * a), c, d].join("-")
          , f = .01 + c / d * 100
          , g = Math.max(1 - (1 - a) / b * (100 - f), a)
          , h = l.substring(0, l.indexOf("Animation")).toLowerCase()
          , i = h && "-" + h + "-" || "";
        return n[e] || (o.insertRule("@" + i + "keyframes " + e + "{0%{opacity:" + g + "}" + f + "%{opacity:" + a + "}" + (f + .01) + "%{opacity:1}" + (f + b) % 100 + "%{opacity:" + a + "}100%{opacity:" + g + "}}", o.cssRules.length),
        n[e] = 1),
        e
    }
    function g(a, b) {
        var d, e, f = a.style;
        if (f[b] !== c)
            return b;
        for (b = b.charAt(0).toUpperCase() + b.slice(1),
        e = 0; e < m.length; e++)
            if (d = m[e] + b,
            f[d] !== c)
                return d
    }
    function h(a, b) {
        for (var c in b)
            a.style[g(a, c) || c] = b[c];
        return a
    }
    function i(a) {
        for (var b = 1; b < arguments.length; b++) {
            var d = arguments[b];
            for (var e in d)
                a[e] === c && (a[e] = d[e])
        }
        return a
    }
    function j(a) {
        for (var b = {
            x: a.offsetLeft,
            y: a.offsetTop
        }; a = a.offsetParent; )
            b.x += a.offsetLeft,
            b.y += a.offsetTop;
        return b
    }
    function k(a) {
        return this.spin ? void (this.opts = i(a || {}, k.defaults, p)) : new k(a)
    }
    var l, m = ["webkit", "Moz", "ms", "O"], n = {}, o = function() {
        var a = d("style", {
            type: "text/css"
        });
        return e(b.getElementsByTagName("head")[0], a),
        a.sheet || a.styleSheet
    }(), p = {
        lines: 12,
        length: 7,
        width: 5,
        radius: 10,
        rotate: 0,
        corners: 1,
        color: "#000",
        speed: 1,
        trail: 100,
        opacity: .25,
        fps: 20,
        zIndex: 2e9,
        className: "spinner",
        top: "auto",
        left: "auto",
        position: "relative"
    };
    k.defaults = {},
    i(k.prototype, {
        spin: function(a) {
            this.stop();
            var b, c, e = this, f = e.opts, g = e.el = h(d(0, {
                className: f.className
            }), {
                position: f.position,
                width: 0,
                zIndex: f.zIndex
            }), i = f.radius + f.length + f.width;
            if (a && (a.insertBefore(g, a.firstChild || null ),
            c = j(a),
            b = j(g),
            h(g, {
                left: ("auto" == f.left ? c.x - b.x + (a.offsetWidth >> 1) : parseInt(f.left, 10) + i) + "px",
                top: ("auto" == f.top ? c.y - b.y + (a.offsetHeight >> 1) : parseInt(f.top, 10) + i) + "px"
            })),
            g.setAttribute("aria-role", "progressbar"),
            e.lines(g, e.opts),
            !l) {
                var k = 0
                  , m = f.fps
                  , n = m / f.speed
                  , o = (1 - f.opacity) / (n * f.trail / 100)
                  , p = n / f.lines;
                !function q() {
                    k++;
                    for (var a = f.lines; a; a--) {
                        var b = Math.max(1 - (k + a * p) % n * o, f.opacity);
                        e.opacity(g, f.lines - a, b, f)
                    }
                    e.timeout = e.el && setTimeout(q, ~~(1e3 / m))
                }()
            }
            return e
        },
        stop: function() {
            var a = this.el;
            return a && (clearTimeout(this.timeout),
            a.parentNode && a.parentNode.removeChild(a),
            this.el = c),
            this
        },
        lines: function(a, b) {
            function c(a, c) {
                return h(d(), {
                    position: "absolute",
                    width: b.length + b.width + "px",
                    height: b.width + "px",
                    background: a,
                    boxShadow: c,
                    transformOrigin: "left",
                    transform: "rotate(" + ~~(360 / b.lines * i + b.rotate) + "deg) translate(" + b.radius + "px,0)",
                    borderRadius: (b.corners * b.width >> 1) + "px"
                })
            }
            for (var g, i = 0; i < b.lines; i++)
                g = h(d(), {
                    position: "absolute",
                    top: 1 + ~(b.width / 2) + "px",
                    transform: b.hwaccel ? "translate3d(0,0,0)" : "",
                    opacity: b.opacity,
                    animation: l && f(b.opacity, b.trail, i, b.lines) + " " + 1 / b.speed + "s linear infinite"
                }),
                b.shadow && e(g, h(c("#000", "0 0 4px #000"), {
                    top: "2px"
                })),
                e(a, e(g, c(b.color, "0 0 1px rgba(0,0,0,.1)")));
            return a
        },
        opacity: function(a, b, c) {
            b < a.childNodes.length && (a.childNodes[b].style.opacity = c)
        }
    }),
    function() {
        function a(a, b) {
            return d("<" + a + ' xmlns="urn:schemas-microsoft.com:vml" class="spin-vml">', b)
        }
        var b = h(d("group"), {
            behavior: "url(#default#VML)"
        });
        !g(b, "transform") && b.adj ? (o.addRule(".spin-vml", "behavior:url(#default#VML)"),
        k.prototype.lines = function(b, c) {
            function d() {
                return h(a("group", {
                    coordsize: j + " " + j,
                    coordorigin: -i + " " + -i
                }), {
                    width: j,
                    height: j
                })
            }
            function f(b, f, g) {
                e(l, e(h(d(), {
                    rotation: 360 / c.lines * b + "deg",
                    left: ~~f
                }), e(h(a("roundrect", {
                    arcsize: c.corners
                }), {
                    width: i,
                    height: c.width,
                    left: c.radius,
                    top: -c.width >> 1,
                    filter: g
                }), a("fill", {
                    color: c.color,
                    opacity: c.opacity
                }), a("stroke", {
                    opacity: 0
                }))))
            }
            var g, i = c.length + c.width, j = 2 * i, k = 2 * -(c.width + c.length) + "px", l = h(d(), {
                position: "absolute",
                top: k,
                left: k
            });
            if (c.shadow)
                for (g = 1; g <= c.lines; g++)
                    f(g, -2, "progid:DXImageTransform.Microsoft.Blur(pixelradius=2,makeshadow=1,shadowopacity=.3)");
            for (g = 1; g <= c.lines; g++)
                f(g);
            return e(b, l)
        }
        ,
        k.prototype.opacity = function(a, b, c, d) {
            var e = a.firstChild;
            d = d.shadow && d.lines || 0,
            e && b + d < e.childNodes.length && (e = e.childNodes[b + d],
            e = e && e.firstChild,
            e = e && e.firstChild,
            e && (e.opacity = c))
        }
        ) : l = g(b, "animation")
    }(),
    "function" == typeof define && define.amd ? define(function() {
        return k
    }) : a.Spinner = k
}(window, document),
function(a) {
    a.fn.spin = function(b, c) {
        var d = {
            Crew: {
                lines: 11,
                length: 7,
                width: 4,
                radius: 7
            },
            tiny: {
                lines: 8,
                length: 2,
                width: 2,
                radius: 3
            },
            small: {
                lines: 8,
                length: 4,
                width: 3,
                radius: 5
            },
            large: {
                lines: 10,
                length: 8,
                width: 4,
                radius: 8
            }
        };
        if (Spinner)
            return this.each(function() {
                var e = a(this)
                  , f = e.data();
                f.spinner && (f.spinner.stop(),
                delete f.spinner),
                b !== !1 && ("string" == typeof b && (b = b in d ? d[b] : {},
                c && (b.color = c)),
                f.spinner = new Spinner(a.extend({
                    color: e.css("color")
                }, b)).spin(this))
            });
        throw "Spinner class not available."
    }
}(jQuery),
function(a) {
    var b, c, d, e, f, g, h, i = "Close", j = "BeforeClose", k = "AfterClose", l = "BeforeAppend", m = "MarkupParse", n = "Open", o = "Change", p = "mfp", q = "." + p, r = "mfp-ready", s = "mfp-removing", t = "mfp-prevent-close", u = function() {}
    , v = !!window.jQuery, w = a(window), x = function(a, c) {
        b.ev.on(p + a + q, c)
    }
    , y = function(b, c, d, e) {
        var f = document.createElement("div");
        return f.className = "mfp-" + b,
        d && (f.innerHTML = d),
        e ? c && c.appendChild(f) : (f = a(f),
        c && f.appendTo(c)),
        f
    }
    , z = function(c, d) {
        b.ev.triggerHandler(p + c, d),
        b.st.callbacks && (c = c.charAt(0).toLowerCase() + c.slice(1),
        b.st.callbacks[c] && b.st.callbacks[c].apply(b, a.isArray(d) ? d : [d]))
    }
    , A = function(c) {
        return c === h && b.currTemplate.closeBtn || (b.currTemplate.closeBtn = a(b.st.closeMarkup.replace("%title%", b.st.tClose)),
        h = c),
        b.currTemplate.closeBtn
    }
    , B = function() {
        a.magnificPopup.instance || (b = new u,
        b.init(),
        a.magnificPopup.instance = b)
    }
    , C = function() {
        var a = document.createElement("p").style
          , b = ["ms", "O", "Moz", "Webkit"];
        if (void 0 !== a.transition)
            return !0;
        for (; b.length; )
            if (b.pop() + "Transition" in a)
                return !0;
        return !1
    }
    ;
    u.prototype = {
        constructor: u,
        init: function() {
            var c = navigator.appVersion;
            b.isIE7 = -1 !== c.indexOf("MSIE 7."),
            b.isIE8 = -1 !== c.indexOf("MSIE 8."),
            b.isLowIE = b.isIE7 || b.isIE8,
            b.isAndroid = /android/gi.test(c),
            b.isIOS = /iphone|ipad|ipod/gi.test(c),
            b.supportsTransition = C(),
            b.probablyMobile = b.isAndroid || b.isIOS || /(Opera Mini)|Kindle|webOS|BlackBerry|(Opera Mobi)|(Windows Phone)|IEMobile/i.test(navigator.userAgent),
            d = a(document.body),
            e = a(document),
            b.popupsCache = {}
        },
        open: function(c) {
            var d;
            if (c.isObj === !1) {
                b.items = c.items.toArray(),
                b.index = 0;
                var f, h = c.items;
                for (d = 0; d < h.length; d++)
                    if (f = h[d],
                    f.parsed && (f = f.el[0]),
                    f === c.el[0]) {
                        b.index = d;
                        break
                    }
            } else
                b.items = a.isArray(c.items) ? c.items : [c.items],
                b.index = c.index || 0;
            if (b.isOpen)
                return void b.updateItemHTML();
            b.types = [],
            g = "",
            c.mainEl && c.mainEl.length ? b.ev = c.mainEl.eq(0) : b.ev = e,
            c.key ? (b.popupsCache[c.key] || (b.popupsCache[c.key] = {}),
            b.currTemplate = b.popupsCache[c.key]) : b.currTemplate = {},
            b.st = a.extend(!0, {}, a.magnificPopup.defaults, c),
            b.fixedContentPos = "auto" === b.st.fixedContentPos ? !b.probablyMobile : b.st.fixedContentPos,
            b.st.modal && (b.st.closeOnContentClick = !1,
            b.st.closeOnBgClick = !1,
            b.st.showCloseBtn = !1,
            b.st.enableEscapeKey = !1),
            b.bgOverlay || (b.bgOverlay = y("bg").on("click" + q, function() {
                b.close()
            }),
            b.wrap = y("wrap").attr("tabindex", -1).on("click" + q, function(a) {
                b._checkIfClose(a.target) && b.close()
            }),
            b.container = y("container", b.wrap)),
            b.contentContainer = y("content"),
            b.st.preloader && (b.preloader = y("preloader", b.container, b.st.tLoading));
            var i = a.magnificPopup.modules;
            for (d = 0; d < i.length; d++) {
                var j = i[d];
                j = j.charAt(0).toUpperCase() + j.slice(1),
                b["init" + j].call(b)
            }
            z("BeforeOpen"),
            b.st.showCloseBtn && (b.st.closeBtnInside ? (x(m, function(a, b, c, d) {
                c.close_replaceWith = A(d.type)
            }),
            g += " mfp-close-btn-in") : b.wrap.append(A())),
            b.st.alignTop && (g += " mfp-align-top"),
            b.fixedContentPos ? b.wrap.css({
                overflow: b.st.overflowY,
                overflowX: "hidden",
                overflowY: b.st.overflowY
            }) : b.wrap.css({
                top: w.scrollTop(),
                position: "absolute"
            }),
            (b.st.fixedBgPos === !1 || "auto" === b.st.fixedBgPos && !b.fixedContentPos) && b.bgOverlay.css({
                height: e.height(),
                position: "absolute"
            }),
            b.st.enableEscapeKey && e.on("keyup" + q, function(a) {
                27 === a.keyCode && b.close()
            }),
            w.on("resize" + q, function() {
                b.updateSize()
            }),
            b.st.closeOnContentClick || (g += " mfp-auto-cursor"),
            g && b.wrap.addClass(g);
            var k = b.wH = w.height()
              , l = {};
            if (b.fixedContentPos && b._hasScrollBar(k)) {
                var o = b._getScrollbarSize();
                o && (l.marginRight = o)
            }
            b.fixedContentPos && (b.isIE7 ? a("body, html").css("overflow", "hidden") : l.overflow = "hidden");
            var p = b.st.mainClass;
            return b.isIE7 && (p += " mfp-ie7"),
            p && b._addClassToMFP(p),
            b.updateItemHTML(),
            z("BuildControls"),
            a("html").css(l),
            b.bgOverlay.add(b.wrap).prependTo(document.body),
            b._lastFocusedEl = document.activeElement,
            setTimeout(function() {
                b.content ? (b._addClassToMFP(r),
                b._setFocus()) : b.bgOverlay.addClass(r),
                e.on("focusin" + q, b._onFocusIn)
            }, 16),
            b.isOpen = !0,
            b.updateSize(k),
            z(n),
            c
        },
        close: function() {
            b.isOpen && (z(j),
            b.isOpen = !1,
            b.st.removalDelay && !b.isLowIE && b.supportsTransition ? (b._addClassToMFP(s),
            setTimeout(function() {
                b._close()
            }, b.st.removalDelay)) : b._close())
        },
        _close: function() {
            z(i);
            var c = s + " " + r + " ";
            if (b.bgOverlay.detach(),
            b.wrap.detach(),
            b.container.empty(),
            b.st.mainClass && (c += b.st.mainClass + " "),
            b._removeClassFromMFP(c),
            b.fixedContentPos) {
                var d = {
                    marginRight: ""
                };
                b.isIE7 ? a("body, html").css("overflow", "") : d.overflow = "",
                a("html").css(d)
            }
            e.off("keyup" + q + " focusin" + q),
            b.ev.off(q),
            b.wrap.attr("class", "mfp-wrap").removeAttr("style"),
            b.bgOverlay.attr("class", "mfp-bg"),
            b.container.attr("class", "mfp-container"),
            !b.st.showCloseBtn || b.st.closeBtnInside && b.currTemplate[b.currItem.type] !== !0 || b.currTemplate.closeBtn && b.currTemplate.closeBtn.detach(),
            b._lastFocusedEl && a(b._lastFocusedEl).focus(),
            b.currItem = null ,
            b.content = null ,
            b.currTemplate = null ,
            b.prevHeight = 0,
            z(k)
        },
        updateSize: function(a) {
            if (b.isIOS) {
                var c = document.documentElement.clientWidth / window.innerWidth
                  , d = window.innerHeight * c;
                b.wrap.css("height", d),
                b.wH = d
            } else
                b.wH = a || w.height();
            b.fixedContentPos || b.wrap.css("height", b.wH),
            z("Resize")
        },
        updateItemHTML: function() {
            var c = b.items[b.index];
            b.contentContainer.detach(),
            b.content && b.content.detach(),
            c.parsed || (c = b.parseEl(b.index));
            var d = c.type;
            if (z("BeforeChange", [b.currItem ? b.currItem.type : "", d]),
            b.currItem = c,
            !b.currTemplate[d]) {
                var e = b.st[d] ? b.st[d].markup : !1;
                z("FirstMarkupParse", e),
                e ? b.currTemplate[d] = a(e) : b.currTemplate[d] = !0
            }
            f && f !== c.type && b.container.removeClass("mfp-" + f + "-holder");
            var g = b["get" + d.charAt(0).toUpperCase() + d.slice(1)](c, b.currTemplate[d]);
            b.appendContent(g, d),
            c.preloaded = !0,
            z(o, c),
            f = c.type,
            b.container.prepend(b.contentContainer),
            z("AfterChange")
        },
        appendContent: function(a, c) {
            b.content = a,
            a ? b.st.showCloseBtn && b.st.closeBtnInside && b.currTemplate[c] === !0 ? b.content.find(".mfp-close").length || b.content.append(A()) : b.content = a : b.content = "",
            z(l),
            b.container.addClass("mfp-" + c + "-holder"),
            b.contentContainer.append(b.content)
        },
        parseEl: function(c) {
            var d = b.items[c]
              , e = d.type;
            if (d = d.tagName ? {
                el: a(d)
            } : {
                data: d,
                src: d.src
            },
            d.el) {
                for (var f = b.types, g = 0; g < f.length; g++)
                    if (d.el.hasClass("mfp-" + f[g])) {
                        e = f[g];
                        break
                    }
                d.src = d.el.attr("data-mfp-src"),
                d.src || (d.src = d.el.attr("href"))
            }
            return d.type = e || b.st.type || "inline",
            d.index = c,
            d.parsed = !0,
            b.items[c] = d,
            z("ElementParse", d),
            b.items[c]
        },
        addGroup: function(a, c) {
            var d = function(d) {
                d.mfpEl = this,
                b._openClick(d, a, c)
            }
            ;
            c || (c = {});
            var e = "click.magnificPopup";
            c.mainEl = a,
            c.items ? (c.isObj = !0,
            a.off(e).on(e, d)) : (c.isObj = !1,
            c.delegate ? a.off(e).on(e, c.delegate, d) : (c.items = a,
            a.off(e).on(e, d)))
        },
        _openClick: function(c, d, e) {
            var f = void 0 !== e.midClick ? e.midClick : a.magnificPopup.defaults.midClick;
            if (f || 2 !== c.which && !c.ctrlKey && !c.metaKey) {
                var g = void 0 !== e.disableOn ? e.disableOn : a.magnificPopup.defaults.disableOn;
                if (g)
                    if (a.isFunction(g)) {
                        if (!g.call(b))
                            return !0
                    } else if (w.width() < g)
                        return !0;
                c.type && (c.preventDefault(),
                b.isOpen && c.stopPropagation()),
                e.el = a(c.mfpEl),
                e.delegate && (e.items = d.find(e.delegate)),
                b.open(e)
            }
        },
        updateStatus: function(a, d) {
            if (b.preloader) {
                c !== a && b.container.removeClass("mfp-s-" + c),
                d || "loading" !== a || (d = b.st.tLoading);
                var e = {
                    status: a,
                    text: d
                };
                z("UpdateStatus", e),
                a = e.status,
                d = e.text,
                b.preloader.html(d),
                b.preloader.find("a").on("click", function(a) {
                    a.stopImmediatePropagation()
                }),
                b.container.addClass("mfp-s-" + a),
                c = a
            }
        },
        _checkIfClose: function(c) {
            if (!a(c).hasClass(t)) {
                var d = b.st.closeOnContentClick
                  , e = b.st.closeOnBgClick;
                if (d && e)
                    return !0;
                if (!b.content || a(c).hasClass("mfp-close") || b.preloader && c === b.preloader[0])
                    return !0;
                if (c === b.content[0] || a.contains(b.content[0], c)) {
                    if (d)
                        return !0
                } else if (e && a.contains(document, c))
                    return !0;
                return !1
            }
        },
        _addClassToMFP: function(a) {
            b.bgOverlay.addClass(a),
            b.wrap.addClass(a)
        },
        _removeClassFromMFP: function(a) {
            this.bgOverlay.removeClass(a),
            b.wrap.removeClass(a)
        },
        _hasScrollBar: function(a) {
            return (b.isIE7 ? e.height() : document.body.scrollHeight) > (a || w.height())
        },
        _setFocus: function() {
            (b.st.focus ? b.content.find(b.st.focus).eq(0) : b.wrap).focus()
        },
        _onFocusIn: function(c) {
            return c.target === b.wrap[0] || a.contains(b.wrap[0], c.target) ? void 0 : (b._setFocus(),
            !1)
        },
        _parseMarkup: function(b, c, d) {
            var e;
            d.data && (c = a.extend(d.data, c)),
            z(m, [b, c, d]),
            a.each(c, function(a, c) {
                if (void 0 === c || c === !1)
                    return !0;
                if (e = a.split("_"),
                e.length > 1) {
                    var d = b.find(q + "-" + e[0]);
                    if (d.length > 0) {
                        var f = e[1];
                        "replaceWith" === f ? d[0] !== c[0] && d.replaceWith(c) : "img" === f ? d.is("img") ? d.attr("src", c) : d.replaceWith('<img src="' + c + '" class="' + d.attr("class") + '" />') : d.attr(e[1], c)
                    }
                } else
                    b.find(q + "-" + a).html(c)
            })
        },
        _getScrollbarSize: function() {
            if (void 0 === b.scrollbarSize) {
                var a = document.createElement("div");
                a.id = "mfp-sbm",
                a.style.cssText = "width: 99px; height: 99px; overflow: scroll; position: absolute; top: -9999px;",
                document.body.appendChild(a),
                b.scrollbarSize = a.offsetWidth - a.clientWidth,
                document.body.removeChild(a)
            }
            return b.scrollbarSize
        }
    },
    a.magnificPopup = {
        instance: null ,
        proto: u.prototype,
        modules: [],
        open: function(b, c) {
            return B(),
            b = b ? a.extend(!0, {}, b) : {},
            b.isObj = !0,
            b.index = c || 0,
            this.instance.open(b)
        },
        close: function() {
            return a.magnificPopup.instance && a.magnificPopup.instance.close()
        },
        registerModule: function(b, c) {
            c.options && (a.magnificPopup.defaults[b] = c.options),
            a.extend(this.proto, c.proto),
            this.modules.push(b)
        },
        defaults: {
            disableOn: 0,
            key: null ,
            midClick: !1,
            mainClass: "",
            preloader: !0,
            focus: "",
            closeOnContentClick: !1,
            closeOnBgClick: !0,
            closeBtnInside: !0,
            showCloseBtn: !0,
            enableEscapeKey: !0,
            modal: !1,
            alignTop: !1,
            removalDelay: 0,
            fixedContentPos: "auto",
            fixedBgPos: "auto",
            overflowY: "auto",
            closeMarkup: '<button title="%title%" type="button" class="mfp-close">&times;</button>',
            tClose: "Close (Esc)",
            tLoading: "Loading..."
        }
    },
    a.fn.magnificPopup = function(c) {
        B();
        var d = a(this);
        if ("string" == typeof c)
            if ("open" === c) {
                var e, f = v ? d.data("magnificPopup") : d[0].magnificPopup, g = parseInt(arguments[1], 10) || 0;
                f.items ? e = f.items[g] : (e = d,
                f.delegate && (e = e.find(f.delegate)),
                e = e.eq(g)),
                b._openClick({
                    mfpEl: e
                }, d, f)
            } else
                b.isOpen && b[c].apply(b, Array.prototype.slice.call(arguments, 1));
        else
            c = a.extend(!0, {}, c),
            v ? d.data("magnificPopup", c) : d[0].magnificPopup = c,
            b.addGroup(d, c);
        return d
    }
    ;
    var D, E, F, G = "inline", H = function() {
        F && (E.after(F.addClass(D)).detach(),
        F = null )
    }
    ;
    a.magnificPopup.registerModule(G, {
        options: {
            hiddenClass: "hide",
            markup: "",
            tNotFound: "Content not found"
        },
        proto: {
            initInline: function() {
                b.types.push(G),
                x(i + "." + G, function() {
                    H()
                })
            },
            getInline: function(c, d) {
                if (H(),
                c.src) {
                    var e = b.st.inline
                      , f = a(c.src);
                    if (f.length) {
                        var g = f[0].parentNode;
                        g && g.tagName && (E || (D = e.hiddenClass,
                        E = y(D),
                        D = "mfp-" + D),
                        F = f.after(E).detach().removeClass(D)),
                        b.updateStatus("ready")
                    } else
                        b.updateStatus("error", e.tNotFound),
                        f = a("<div>");
                    return c.inlineElement = f,
                    f
                }
                return b.updateStatus("ready"),
                b._parseMarkup(d, {}, c),
                d
            }
        }
    });
    var I, J = "ajax", K = function() {
        I && d.removeClass(I)
    }
    , L = function() {
        K(),
        b.req && b.req.abort()
    }
    ;
    a.magnificPopup.registerModule(J, {
        options: {
            settings: null ,
            cursor: "mfp-ajax-cur",
            tError: '<a href="%url%">The content</a> could not be loaded.'
        },
        proto: {
            initAjax: function() {
                b.types.push(J),
                I = b.st.ajax.cursor,
                x(i + "." + J, L),
                x("BeforeChange." + J, L)
            },
            getAjax: function(c) {
                I && d.addClass(I),
                b.updateStatus("loading");
                var e = a.extend({
                    url: c.src,
                    success: function(d, e, f) {
                        var g = {
                            data: d,
                            xhr: f
                        };
                        z("ParseAjax", g),
                        b.appendContent(a(g.data), J),
                        c.finished = !0,
                        K(),
                        b._setFocus(),
                        setTimeout(function() {
                            b.wrap.addClass(r)
                        }, 16),
                        b.updateStatus("ready"),
                        z("AjaxContentAdded")
                    },
                    error: function() {
                        K(),
                        c.finished = c.loadError = !0,
                        b.updateStatus("error", b.st.ajax.tError.replace("%url%", c.src))
                    }
                }, b.st.ajax.settings);
                return b.req = a.ajax(e),
                ""
            }
        }
    });
    var M, N = function(c) {
        if (c.data && void 0 !== c.data.title)
            return c.data.title;
        var d = b.st.image.titleSrc;
        if (d) {
            if (a.isFunction(d))
                return d.call(b, c);
            if (c.el)
                return c.el.attr(d) || ""
        }
        return ""
    }
    ;
    a.magnificPopup.registerModule("image", {
        options: {
            markup: '<div class="mfp-figure"><div class="mfp-close"></div><figure><div class="mfp-img"></div><figcaption><div class="mfp-bottom-bar"><div class="mfp-title"></div><div class="mfp-counter"></div></div></figcaption></figure></div>',
            cursor: "mfp-zoom-out-cur",
            titleSrc: "title",
            verticalFit: !0,
            tError: '<a href="%url%">The image</a> could not be loaded.'
        },
        proto: {
            initImage: function() {
                var a = b.st.image
                  , c = ".image";
                b.types.push("image"),
                x(n + c, function() {
                    "image" === b.currItem.type && a.cursor && d.addClass(a.cursor)
                }),
                x(i + c, function() {
                    a.cursor && d.removeClass(a.cursor),
                    w.off("resize" + q)
                }),
                x("Resize" + c, b.resizeImage),
                b.isLowIE && x("AfterChange", b.resizeImage)
            },
            resizeImage: function() {
                var a = b.currItem;
                if (a && a.img && b.st.image.verticalFit) {
                    var c = 0;
                    b.isLowIE && (c = parseInt(a.img.css("padding-top"), 10) + parseInt(a.img.css("padding-bottom"), 10)),
                    a.img.css("max-height", b.wH - c)
                }
            },
            _onImageHasSize: function(a) {
                a.img && (a.hasSize = !0,
                M && clearInterval(M),
                a.isCheckingImgSize = !1,
                z("ImageHasSize", a),
                a.imgHidden && (b.content && b.content.removeClass("mfp-loading"),
                a.imgHidden = !1))
            },
            findImageSize: function(a) {
                var c = 0
                  , d = a.img[0]
                  , e = function(f) {
                    M && clearInterval(M),
                    M = setInterval(function() {
                        return d.naturalWidth > 0 ? void b._onImageHasSize(a) : (c > 200 && clearInterval(M),
                        c++,
                        void (3 === c ? e(10) : 40 === c ? e(50) : 100 === c && e(500)))
                    }, f)
                }
                ;
                e(1)
            },
            getImage: function(c, d) {
                var e = 0
                  , f = function() {
                    c && (c.img[0].complete ? (c.img.off(".mfploader"),
                    c === b.currItem && (b._onImageHasSize(c),
                    b.updateStatus("ready")),
                    c.hasSize = !0,
                    c.loaded = !0,
                    z("ImageLoadComplete")) : (e++,
                    200 > e ? setTimeout(f, 100) : g()))
                }
                  , g = function() {
                    c && (c.img.off(".mfploader"),
                    c === b.currItem && (b._onImageHasSize(c),
                    b.updateStatus("error", h.tError.replace("%url%", c.src))),
                    c.hasSize = !0,
                    c.loaded = !0,
                    c.loadError = !0)
                }
                  , h = b.st.image
                  , i = d.find(".mfp-img");
                if (i.length) {
                    var j = document.createElement("img");
                    j.className = "mfp-img",
                    c.img = a(j).on("load.mfploader", f).on("error.mfploader", g),
                    j.src = c.src,
                    i.is("img") && (c.img = c.img.clone()),
                    c.img[0].naturalWidth > 0 && (c.hasSize = !0)
                }
                return b._parseMarkup(d, {
                    title: N(c),
                    img_replaceWith: c.img
                }, c),
                b.resizeImage(),
                c.hasSize ? (M && clearInterval(M),
                c.loadError ? (d.addClass("mfp-loading"),
                b.updateStatus("error", h.tError.replace("%url%", c.src))) : (d.removeClass("mfp-loading"),
                b.updateStatus("ready")),
                d) : (b.updateStatus("loading"),
                c.loading = !0,
                c.hasSize || (c.imgHidden = !0,
                d.addClass("mfp-loading"),
                b.findImageSize(c)),
                d)
            }
        }
    });
    var O, P = function() {
        return void 0 === O && (O = void 0 !== document.createElement("p").style.MozTransform),
        O
    }
    ;
    a.magnificPopup.registerModule("zoom", {
        options: {
            enabled: !1,
            easing: "ease-in-out",
            duration: 300,
            opener: function(a) {
                return a.is("img") ? a : a.find("img")
            }
        },
        proto: {
            initZoom: function() {
                var a, c = b.st.zoom, d = ".zoom";
                if (c.enabled && b.supportsTransition) {
                    var e, f, g = c.duration, h = function(a) {
                        var b = a.clone().removeAttr("style").removeAttr("class").addClass("mfp-animated-image")
                          , d = "all " + c.duration / 1e3 + "s " + c.easing
                          , e = {
                            position: "fixed",
                            zIndex: 9999,
                            left: 0,
                            top: 0,
                            "-webkit-backface-visibility": "hidden"
                        }
                          , f = "transition";
                        return e["-webkit-" + f] = e["-moz-" + f] = e["-o-" + f] = e[f] = d,
                        b.css(e),
                        b
                    }
                    , k = function() {
                        b.content.css("visibility", "visible")
                    }
                    ;
                    x("BuildControls" + d, function() {
                        if (b._allowZoom()) {
                            if (clearTimeout(e),
                            b.content.css("visibility", "hidden"),
                            a = b._getItemToZoom(),
                            !a)
                                return void k();
                            f = h(a),
                            f.css(b._getOffset()),
                            b.wrap.append(f),
                            e = setTimeout(function() {
                                f.css(b._getOffset(!0)),
                                e = setTimeout(function() {
                                    k(),
                                    setTimeout(function() {
                                        f.remove(),
                                        a = f = null ,
                                        z("ZoomAnimationEnded")
                                    }, 16)
                                }, g)
                            }, 16)
                        }
                    }),
                    x(j + d, function() {
                        if (b._allowZoom()) {
                            if (clearTimeout(e),
                            b.st.removalDelay = g,
                            !a) {
                                if (a = b._getItemToZoom(),
                                !a)
                                    return;
                                f = h(a)
                            }
                            f.css(b._getOffset(!0)),
                            b.wrap.append(f),
                            b.content.css("visibility", "hidden"),
                            setTimeout(function() {
                                f.css(b._getOffset())
                            }, 16)
                        }
                    }),
                    x(i + d, function() {
                        b._allowZoom() && (k(),
                        f && f.remove(),
                        a = null )
                    })
                }
            },
            _allowZoom: function() {
                return "image" === b.currItem.type
            },
            _getItemToZoom: function() {
                return b.currItem.hasSize ? b.currItem.img : !1
            },
            _getOffset: function(c) {
                var d;
                d = c ? b.currItem.img : b.st.zoom.opener(b.currItem.el || b.currItem);
                var e = d.offset()
                  , f = parseInt(d.css("padding-top"), 10)
                  , g = parseInt(d.css("padding-bottom"), 10);
                e.top -= a(window).scrollTop() - f;
                var h = {
                    width: d.width(),
                    height: (v ? d.innerHeight() : d[0].offsetHeight) - g - f
                };
                return P() ? h["-moz-transform"] = h.transform = "translate(" + e.left + "px," + e.top + "px)" : (h.left = e.left,
                h.top = e.top),
                h
            }
        }
    });
    var Q = "iframe"
      , R = "//about:blank"
      , S = function(a) {
        if (b.currTemplate[Q]) {
            var c = b.currTemplate[Q].find("iframe");
            c.length && (a || (c[0].src = R),
            b.isIE8 && c.css("display", a ? "block" : "none"))
        }
    }
    ;
    a.magnificPopup.registerModule(Q, {
        options: {
            markup: '<div class="mfp-iframe-scaler"><div class="mfp-close"></div><iframe class="mfp-iframe" src="//about:blank" frameborder="0" allowfullscreen></iframe></div>',
            srcAction: "iframe_src",
            patterns: {
                youtube: {
                    index: "youtube.com",
                    id: "v=",
                    src: "//www.youtube.com/embed/%id%?autoplay=1"
                },
                vimeo: {
                    index: "vimeo.com/",
                    id: "/",
                    src: "//player.vimeo.com/video/%id%?autoplay=1"
                },
                gmaps: {
                    index: "//maps.google.",
                    src: "%id%&output=embed"
                }
            }
        },
        proto: {
            initIframe: function() {
                b.types.push(Q),
                x("BeforeChange", function(a, b, c) {
                    b !== c && (b === Q ? S() : c === Q && S(!0))
                }),
                x(i + "." + Q, function() {
                    S()
                })
            },
            getIframe: function(c, d) {
                var e = c.src
                  , f = b.st.iframe;
                a.each(f.patterns, function() {
                    return e.indexOf(this.index) > -1 ? (this.id && (e = "string" == typeof this.id ? e.substr(e.lastIndexOf(this.id) + this.id.length, e.length) : this.id.call(this, e)),
                    e = this.src.replace("%id%", e),
                    !1) : void 0
                });
                var g = {};
                return f.srcAction && (g[f.srcAction] = e),
                b._parseMarkup(d, g, c),
                b.updateStatus("ready"),
                d
            }
        }
    });
    var T = function(a) {
        var c = b.items.length;
        return a > c - 1 ? a - c : 0 > a ? c + a : a
    }
      , U = function(a, b, c) {
        return a.replace(/%curr%/gi, b + 1).replace(/%total%/gi, c)
    }
    ;
    a.magnificPopup.registerModule("gallery", {
        options: {
            enabled: !1,
            arrowMarkup: '<button title="%title%" type="button" class="mfp-arrow mfp-arrow-%dir%"></button>',
            preload: [0, 2],
            navigateByImgClick: !0,
            arrows: !0,
            tPrev: "Previous (Left arrow key)",
            tNext: "Next (Right arrow key)",
            tCounter: "%curr% of %total%"
        },
        proto: {
            initGallery: function() {
                var c = b.st.gallery
                  , d = ".mfp-gallery"
                  , f = Boolean(a.fn.mfpFastClick);
                return b.direction = !0,
                c && c.enabled ? (g += " mfp-gallery",
                x(n + d, function() {
                    c.navigateByImgClick && b.wrap.on("click" + d, ".mfp-img", function() {
                        return b.items.length > 1 ? (b.next(),
                        !1) : void 0
                    }),
                    e.on("keydown" + d, function(a) {
                        37 === a.keyCode ? b.prev() : 39 === a.keyCode && b.next()
                    })
                }),
                x("UpdateStatus" + d, function(a, c) {
                    c.text && (c.text = U(c.text, b.currItem.index, b.items.length))
                }),
                x(m + d, function(a, d, e, f) {
                    var g = b.items.length;
                    e.counter = g > 1 ? U(c.tCounter, f.index, g) : ""
                }),
                x("BuildControls" + d, function() {
                    if (b.items.length > 1 && c.arrows && !b.arrowLeft) {
                        var d = c.arrowMarkup
                          , e = b.arrowLeft = a(d.replace(/%title%/gi, c.tPrev).replace(/%dir%/gi, "left")).addClass(t)
                          , g = b.arrowRight = a(d.replace(/%title%/gi, c.tNext).replace(/%dir%/gi, "right")).addClass(t)
                          , h = f ? "mfpFastClick" : "click";
                        e[h](function() {
                            b.prev()
                        }),
                        g[h](function() {
                            b.next()
                        }),
                        b.isIE7 && (y("b", e[0], !1, !0),
                        y("a", e[0], !1, !0),
                        y("b", g[0], !1, !0),
                        y("a", g[0], !1, !0)),
                        b.container.append(e.add(g))
                    }
                }),
                x(o + d, function() {
                    b._preloadTimeout && clearTimeout(b._preloadTimeout),
                    b._preloadTimeout = setTimeout(function() {
                        b.preloadNearbyImages(),
                        b._preloadTimeout = null 
                    }, 16)
                }),
                void x(i + d, function() {
                    e.off(d),
                    b.wrap.off("click" + d),
                    b.arrowLeft && f && b.arrowLeft.add(b.arrowRight).destroyMfpFastClick(),
                    b.arrowRight = b.arrowLeft = null 
                })) : !1
            },
            next: function() {
                b.direction = !0,
                b.index = T(b.index + 1),
                b.updateItemHTML()
            },
            prev: function() {
                b.direction = !1,
                b.index = T(b.index - 1),
                b.updateItemHTML()
            },
            goTo: function(a) {
                b.direction = a >= b.index,
                b.index = a,
                b.updateItemHTML()
            },
            preloadNearbyImages: function() {
                var a, c = b.st.gallery.preload, d = Math.min(c[0], b.items.length), e = Math.min(c[1], b.items.length);
                for (a = 1; a <= (b.direction ? e : d); a++)
                    b._preloadItem(b.index + a);
                for (a = 1; a <= (b.direction ? d : e); a++)
                    b._preloadItem(b.index - a)
            },
            _preloadItem: function(c) {
                if (c = T(c),
                !b.items[c].preloaded) {
                    var d = b.items[c];
                    d.parsed || (d = b.parseEl(c)),
                    z("LazyLoad", d),
                    "image" === d.type && (d.img = a('<img class="mfp-img" />').on("load.mfploader", function() {
                        d.hasSize = !0
                    }).on("error.mfploader", function() {
                        d.hasSize = !0,
                        d.loadError = !0,
                        z("LazyLoadError", d)
                    }).attr("src", d.src)),
                    d.preloaded = !0
                }
            }
        }
    });
    var V = "retina";
    a.magnificPopup.registerModule(V, {
        options: {
            replaceSrc: function(a) {
                return a.src.replace(/\.\w+$/, function(a) {
                    return "@2x" + a
                })
            },
            ratio: 1
        },
        proto: {
            initRetina: function() {
                if (window.devicePixelRatio > 1) {
                    var a = b.st.retina
                      , c = a.ratio;
                    c = isNaN(c) ? c() : c,
                    c > 1 && (x("ImageHasSize." + V, function(a, b) {
                        b.img.css({
                            "max-width": b.img[0].naturalWidth / c,
                            width: "100%"
                        })
                    }),
                    x("ElementParse." + V, function(b, d) {
                        d.src = a.replaceSrc(d, c)
                    }))
                }
            }
        }
    }),
    function() {
        var b = 1e3
          , c = "ontouchstart" in window
          , d = function() {
            w.off("touchmove" + f + " touchend" + f)
        }
          , e = "mfpFastClick"
          , f = "." + e;
        a.fn.mfpFastClick = function(e) {
            return a(this).each(function() {
                var g, h = a(this);
                if (c) {
                    var i, j, k, l, m, n;
                    h.on("touchstart" + f, function(a) {
                        l = !1,
                        n = 1,
                        m = a.originalEvent ? a.originalEvent.touches[0] : a.touches[0],
                        j = m.clientX,
                        k = m.clientY,
                        w.on("touchmove" + f, function(a) {
                            m = a.originalEvent ? a.originalEvent.touches : a.touches,
                            n = m.length,
                            m = m[0],
                            (Math.abs(m.clientX - j) > 10 || Math.abs(m.clientY - k) > 10) && (l = !0,
                            d())
                        }).on("touchend" + f, function(a) {
                            d(),
                            l || n > 1 || (g = !0,
                            a.preventDefault(),
                            clearTimeout(i),
                            i = setTimeout(function() {
                                g = !1
                            }, b),
                            e())
                        })
                    })
                }
                h.on("click" + f, function() {
                    g || e()
                })
            })
        }
        ,
        a.fn.destroyMfpFastClick = function() {
            a(this).off("touchstart" + f + " click" + f),
            c && w.off("touchmove" + f + " touchend" + f)
        }
    }(),
    B()
}(window.jQuery || window.Zepto),
function(a) {
    "use strict";
    a.fn.bigSlide = function(b) {
        var c = a.extend({
            menu: "#menu",
            push: ".push",
            side: "left",
            menuWidth: "15.625em",
            speed: "300"
        }, b)
          , d = this
          , e = a(c.menu)
          , f = a(c.push)
          , g = c.menuWidth
          , h = {
            position: "fixed",
            top: "0",
            bottom: "0",
            "settings.side": "-" + c.menuWidth,
            width: c.menuWidth,
            height: "100%"
        }
          , i = {
            "-webkit-transition": c.side + " " + c.speed + "ms ease",
            "-moz-transition": c.side + " " + c.speed + "ms ease",
            "-ms-transition": c.side + " " + c.speed + "ms ease",
            "-o-transition": c.side + " " + c.speed + "ms ease",
            transition: c.side + " " + c.speed + "ms ease"
        };
        return e.css(h),
        f.css(c.side, "0"),
        e.css(i),
        f.css(i),
        e._state = "closed",
        e.open = function() {
            e._state = "open",
            e.css(c.side, "0"),
            f.css(c.side, g)
        }
        ,
        e.close = function() {
            e._state = "closed",
            e.css(c.side, "-" + g),
            f.css(c.side, "0")
        }
        ,
        d.on("click.bigSlide", function(a) {
            a.preventDefault(),
            "closed" === e._state ? e.open() : e.close()
        }),
        d.on("touchend", function(a) {
            d.trigger("click.bigSlide"),
            a.preventDefault()
        }),
        e
    }
}(jQuery),
function(a) {
    a.fn.simplyCountable = function(b) {
        b = a.extend({
            counter: "#counter",
            countType: "characters",
            wordSeparator: " ",
            maxCount: 140,
            strictMax: !1,
            countDirection: "down",
            safeClass: "safe",
            overClass: "over",
            thousandSeparator: ",",
            onOverCount: function() {},
            onSafeCount: function() {},
            onMaxCount: function() {}
        }, b);
        var c = this
          , d = a(b.counter);
        if (!d.length)
            return !1;
        regex = new RegExp("[" + b.wordSeparator + "]+");
        var e = function() {
            var e, f, g = function(a) {
                return a - 2 * a + b.maxCount
            }
            , h = function() {
                return "up" === b.countDirection ? f : e
            }
            , i = function(a) {
                var c = "";
                if (b.thousandSeparator) {
                    a = a.toString(),
                    a.match(/^-/) && (a = a.substr(1),
                    c = "-");
                    for (var d = a.length - 3; d > 0; d -= 3)
                        a = a.substr(0, d) + b.thousandSeparator + a.substr(d)
                }
                return c + a
            }
            ;
            if ("words" === b.countType ? (e = b.maxCount - a.trim(c.val()).split(regex).length,
            "" === c.val() && (e += 1)) : e = b.maxCount - c.val().length,
            f = g(e),
            b.strictMax && 0 >= e) {
                var j = c.val();
                (0 > e || j.match(new RegExp("[" + b.wordSeparator + "]$"))) && b.onMaxCount(h(), c, d),
                "words" === b.countType ? c.val(j.split(regex).slice(0, b.maxCount).join(b.wordSeparator)) : c.val(j.substring(0, b.maxCount)),
                e = 0,
                f = b.maxCount
            }
            d.text(i(h())),
            d.hasClass(b.safeClass) || d.hasClass(b.overClass) ? 0 > e && d.hasClass(b.safeClass) ? (d.removeClass(b.safeClass).addClass(b.overClass),
            b.onOverCount(h(), c, d)) : e >= 0 && d.hasClass(b.overClass) && (d.removeClass(b.overClass).addClass(b.safeClass),
            b.onSafeCount(h(), c, d)) : 0 > e ? d.addClass(b.overClass) : d.addClass(b.safeClass)
        }
        ;
        e(),
        c.keyup(e),
        c.bind("paste", function() {
            setTimeout(e, 5)
        })
    }
}(jQuery),
jQuery(function(a) {
    function b(a, b) {
        a.append(b)
    }
    function c(a, b) {
        a.html(b)
    }
    window.filepicker && filepicker.setKey("A2OcnhikKSCHef5YYk0qgz");
    var d = "/img/app/generic-file.png"
      , e = function(d) {
        var e = this;
        return this.$input = a(d),
        this.$container = this.$input.parents(".js-fp-container"),
        this.$results = this.$container.find(".js-fp-results"),
        this.$form = this.$input.parents("form"),
        this.$submitButton = this.$form.find('[type="submit"]'),
        this.config = {
            multiple: this.$input.is("[multiple]"),
            onlyImages: this.$input.is("[data-only-images=true]"),
            model: this.$input.data("model"),
            modelAssociation: this.$input.data("model-association")
        },
        this.idString = this.formatIdString(),
        this.nameString = this.formatNameString(),
        this.renderContent = this.config.multiple ? b : c,
        this.$input.change(function(b) {
            var c = a(b.target)
              , d = c.get(0).files
              , f = a(".js-fp-file").map(function() {
                return 1 * a(this).data("index")
            })
              , g = Math.max.apply(Math, f) + 1;
            isFinite(g) || (g = 0),
            a.each(d, function(a, b) {
                e.readFile(a + g, b)
            })
        }),
        this.$container.on("click", ".js-fp-remove", function() {
            var b = a(this);
            return e.removeFile(b.parents(".js-fp-file"), b.data("index")),
            !1
        }),
        this
    }
    ;
    e.prototype.formatBytes = function(a) {
        var b = ["Bytes", "KB", "MB", "GB", "TB"];
        if (0 === a)
            return "0 Bytes";
        var c = parseInt(Math.floor(Math.log(a) / Math.log(1024)));
        return Math.round(a / Math.pow(1024, c), 2) + " " + b[c]
    }
    ,
    e.prototype.formatIdString = function() {
        var a = "";
        return a += this.config.model || "",
        a += this.config.modelAssociation || ""
    }
    ,
    e.prototype.formatNameString = function() {
        var a = "";
        return a += this.config.model ? "[" + this.config.model + "]" : "",
        a += this.config.modelAssociation ? "[" + this.config.modelAssociation + "]" : ""
    }
    ,
    e.prototype.interpolateContent = function(a, b, c) {
        return window.Templates["fp-content"]({
            file: b,
            index: a,
            src: c
        })
    }
    ,
    e.prototype.interpolateForm = function(a, b) {
        return window.Templates["fp-form"]({
            InkBlob: b,
            index: a,
            idString: this.idString,
            nameString: this.nameString
        })
    }
    ,
    e.prototype.readFile = function(a, b) {
        var c, e = this;
        b.sizeString = this.formatBytes(b.size),
        -1 !== b.type.indexOf("image") && this.isPreviewable(b.type) ? filepicker.read(b, {
            base64encode: !0
        }, function(d) {
            c = "data:" + b.type + ";base64," + d,
            e.processFile(a, b, c)
        }, function(c) {
            e.processFile(a, b, d),
            e.handleError(c)
        }) : e.processFile(a, b, d)
    }
    ,
    e.prototype.isPreviewable = function(a) {
        var b = ["png", "jpg", "jpeg", "tiff", "gif"];
        return _.some(b, function(b) {
            return -1 !== a.indexOf(b)
        })
    }
    ,
    e.prototype.processFile = function(a, b, c) {
        this.renderContent(this.$results, this.interpolateContent(a, b, c)),
        this.storeFile(a, b)
    }
    ,
    e.prototype.storeFile = function(a, b) {
        var c = this
          , d = this.$results.find(".js-fp-file").filter("[data-index=" + a + "]")
          , e = d.find(".js-fp-progress")
          , f = d.find(".js-fp-data")
          , g = {
            access: "public"
        };
        c.$submitButton.attr("disabled", !0),
        filepicker.store(b, g, function(b) {
            f.html(c.interpolateForm(a, b)),
            e.addClass("progress-bar-success"),
            setTimeout(function() {
                c.removeElement(e.parent()),
                c.$submitButton.attr("disabled", !1)
            }, 1500)
        }, this.handleError, function(a) {
            e.css("width", a + "%")
        })
    }
    ,
    e.prototype.removeFile = function(a, b) {
        this.removeElement(a),
        this.config.multiple || this.$input.val(null ),
        this.$submitButton.attr("disabled", !1)
    }
    ,
    e.prototype.removeElement = function(b) {
        b.slideUp().queue(function() {
            a(this).remove().dequeue()
        })
    }
    ,
    e.prototype.handleError = function(a) {
        console.log(a)
    }
    ,
    e.init = function() {
        a(".js-fp-input").each(function(a, b) {
            new e(b)
        })
    }
    ,
    e.init(),
    a(document.body).on("view.reloaded", function() {
        setTimeout(function() {
            e.init()
        }, 5)
    })
}),
function(a, b, c) {
    function d(a) {
        var c, d, e, f;
        c = b(a.target),
        e = c.text(),
        f = c.data("alt-text"),
        f && (c.text(f),
        c.data("alt-text", e)),
        d = c.parents(".js-collapse-container"),
        d.find(".js-shortened").toggle(),
        d.find(".js-collapse-full").toggle(),
        a.preventDefault()
    }
    b(function() {
        b(document.body).on("click", ".js-collapse-action", d)
    })
}(window, jQuery),
$(".js-opening-rating-form").click(function(a) {
    $(this).hasClass("js-opening-rating-form-reset") || a.preventDefault();
    var b = "#" + $(this).data("target");
    $(b).stop().slideToggle()
}),
function(a, b, c) {
    b(function() {
        b("body").tooltip({
            selector: "[data-toggle=tooltip]",
            trigger: "hover"
        })
    })
}(window, jQuery),
function(a, b, c) {
    var d, e, f, g, h, i;
    e = d = {},
    f = {},
    g = {},
    h = "[data-disable-button=true]",
    i = "Sending..",
    d.setup = function() {
        this.$body = b(document.body),
        this.$buttons = b(h)
    }
    ,
    f.setup = function() {
        e.$body.on("click", h, function(a) {
            var c = b(a.target);
            return c.addClass("disabled").text(i).val(i),
            f.destroy(),
            g.setup(),
            !0
        })
    }
    ,
    f.destroy = function() {
        e.$body.off("click", h)
    }
    ,
    g.setup = function() {
        e.$body.on("click", h, function(a) {
            return a.preventDefault(),
            !1
        })
    }
    ,
    g.destroy = function() {
        e.$body.off("click", h)
    }
    ,
    b(function() {
        d.setup(),
        f.setup()
    })
}(window, jQuery),
function(a, b, c) {
    var d, e, f, g;
    e = d = {},
    f = {},
    g = {},
    d.init = function() {
        this.$close = b(".js-close-flash"),
        this.$flash = b(".js-flash")
    }
    ,
    f.init = function() {
        this.initListeners()
    }
    ,
    g.init = function() {
        this.isHovering = !1
    }
    ,
    f.initListeners = function() {
        e.$flash.hasClass("js-flash-error") || (f.mouseoverFlashListener(),
        f.mouseoutFlashListener(),
        f.waitAndClose()),
        f.closeFlashListener()
    }
    ,
    f.closeFlashListener = function() {
        e.$flash.on("click", ".js-close-flash", function(a) {
            a.preventDefault(),
            f.closeFlash(),
            f.closeFlashListener.destroy()
        })
    }
    ,
    f.closeFlashListener.destroy = function() {
        e.$flash.off("click")
    }
    ,
    f.mouseoverFlashListener = function() {
        e.$flash.on("mouseover", function() {
            g.isHovering = !0
        })
    }
    ,
    f.mouseoverFlashListener.destroy = function() {
        e.$flash.off("mouseover")
    }
    ,
    f.mouseoutFlashListener = function() {
        e.$flash.on("mouseout", function() {
            g.isHovering = !1
        })
    }
    ,
    f.mouseoutFlashListener.destroy = function() {
        e.$flash.off("mouseout")
    }
    ,
    f.closeFlash = function() {
        e.$flash.fadeOut(200)
    }
    ,
    f.waitAndClose = function() {
        setTimeout(function() {
            g.isHovering ? f.waitAndClose() : (f.closeFlash(),
            f.mouseoverFlashListener.destroy(),
            f.mouseoutFlashListener.destroy())
        }, 4e3)
    }
    ,
    b(function() {
        d.init(),
        f.init(),
        g.init()
    })
}(window, jQuery),
this.Templates = this.Templates || {},
this.Templates["fp-content"] = function(obj) {
    obj || (obj = {});
    var __t, __p = "";
    _.escape;
    with (obj)
        __p += '<div class="row padding-vertical-small js-fp-file" data-index="' + (null  == (__t = index) ? "" : __t) + '">\n\n    <div class="col-xs-3">\n        <img class="img-responsive js-fp-src" src="' + (null  == (__t = src) ? "" : __t) + '" style="max-height: 150px;">\n    </div> <!-- close .col -->\n\n    <div class="col-xs-9">\n        <div class="row">\n            <div class="col-xs-8">\n                <h5 class="zeta text-primary js-fp-filename">' + (null  == (__t = file.name) ? "" : __t) + '</h5>\n                <h5 class="zeta text-tertiary js-fp-size">' + (null  == (__t = file.sizeString) ? "" : __t) + '</h5>\n            </div> <!-- close .col -->\n            <div class="col-xs-4 text-right">\n                <a href="#" class="zeta text-danger js-fp-remove" data-index="' + (null  == (__t = index) ? "" : __t) + '">Remove</a>\n            </div> <!-- close .col -->\n        </div> <!-- close .row -->\n        <div class="progress">\n            <div class="progress-bar js-fp-progress" role="progressbar" style="width: 0%;"></div>\n        </div> <!-- close .progress -->\n        <div class="js-fp-data" data-index="' + (null  == (__t = index) ? "" : __t) + '">\n        </div> <!-- close .js-fp-data -->\n    </div> <!-- close .col -->\n</div> <!-- close .row -->';
    return __p
}
,
this.Templates = this.Templates || {},
this.Templates["fp-form"] = function(obj) {
    obj || (obj = {});
    var __t, __p = "";
    _.escape;
    with (obj)
        __p += '<div class="js-fp-hidden-form">\n    <input type="hidden" id="FilePicker' + (null  == (__t = idString) ? "" : __t) + (null  == (__t = index) ? "" : __t) + 'Size" name="data[FilePicker]' + (null  == (__t = nameString) ? "" : __t) + "[" + (null  == (__t = index) ? "" : __t) + '][size]" value="' + (null  == (__t = InkBlob.size) ? "" : __t) + '">\n\n    <input type="hidden" id="FilePicker' + (null  == (__t = idString) ? "" : __t) + (null  == (__t = index) ? "" : __t) + 'FilePickerURL" name="data[FilePicker]' + (null  == (__t = nameString) ? "" : __t) + "[" + (null  == (__t = index) ? "" : __t) + '][file_picker_url]" value="' + (null  == (__t = InkBlob.url) ? "" : __t) + '">\n\n    <input type="hidden" id="FilePicker' + (null  == (__t = idString) ? "" : __t) + (null  == (__t = index) ? "" : __t) + 'Filename" name="data[FilePicker]' + (null  == (__t = nameString) ? "" : __t) + "[" + (null  == (__t = index) ? "" : __t) + '][filename]" value="' + (null  == (__t = InkBlob.filename) ? "" : __t) + '">\n\n    <input type="hidden" id="FilePicker' + (null  == (__t = idString) ? "" : __t) + (null  == (__t = index) ? "" : __t) + 'Mimetype" name="data[FilePicker]' + (null  == (__t = nameString) ? "" : __t) + "[" + (null  == (__t = index) ? "" : __t) + '][mimetype]" value="' + (null  == (__t = InkBlob.mimetype) ? "" : __t) + '">\n    \n    <input type="hidden" id="FilePicker' + (null  == (__t = idString) ? "" : __t) + (null  == (__t = index) ? "" : __t) + 'Key" name="data[FilePicker]' + (null  == (__t = nameString) ? "" : __t) + "[" + (null  == (__t = index) ? "" : __t) + '][key]" value="' + (null  == (__t = InkBlob.key) ? "" : __t) + '">\n</div> <!-- close .js-fp-hidden-form -->';
    return __p
}
;
