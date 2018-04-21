!function(a, b) {
    if ("function" == typeof define && define.amd)
        define(["exports", "module"], b);
    else if ("undefined" != typeof exports && "undefined" != typeof module)
        b(exports, module);
    else {
        var c = {
            exports: {}
        };
        b(c.exports, c),
        a.autosize = c.exports
    }
}(this, function(a, b) {
    "use strict";
    function c(a) {
        function b() {
            var b = window.getComputedStyle(a, null );
            n = b.overflowY,
            "vertical" === b.resize ? a.style.resize = "none" : "both" === b.resize && (a.style.resize = "horizontal"),
            m = "content-box" === b.boxSizing ? -(parseFloat(b.paddingTop) + parseFloat(b.paddingBottom)) : parseFloat(b.borderTopWidth) + parseFloat(b.borderBottomWidth),
            isNaN(m) && (m = 0),
            e()
        }
        function c(b) {
            var c = a.style.width;
            a.style.width = "0px",
            a.offsetWidth,
            a.style.width = c,
            n = b,
            l && (a.style.overflowY = b),
            d()
        }
        function d() {
            var b = window.pageYOffset
              , c = document.body.scrollTop
              , d = a.style.height;
            a.style.height = "auto";
            var e = a.scrollHeight + m;
            return 0 === a.scrollHeight ? void (a.style.height = d) : (a.style.height = e + "px",
            o = a.clientWidth,
            document.documentElement.scrollTop = b,
            void (document.body.scrollTop = c))
        }
        function e() {
            var b = a.style.height;
            d();
            var e = window.getComputedStyle(a, null );
            if (e.height !== a.style.height ? "visible" !== n && c("visible") : "hidden" !== n && c("hidden"),
            b !== a.style.height) {
                var f = g("autosize:resized");
                a.dispatchEvent(f)
            }
        }
        var h = void 0 === arguments[1] ? {} : arguments[1]
          , i = h.setOverflowX
          , j = void 0 === i ? !0 : i
          , k = h.setOverflowY
          , l = void 0 === k ? !0 : k;
        if (a && a.nodeName && "TEXTAREA" === a.nodeName && !f.has(a)) {
            var m = null 
              , n = null 
              , o = a.clientWidth
              , p = function() {
                a.clientWidth !== o && e()
            }
              , q = function(b) {
                window.removeEventListener("resize", p, !1),
                a.removeEventListener("input", e, !1),
                a.removeEventListener("keyup", e, !1),
                a.removeEventListener("autosize:destroy", q, !1),
                a.removeEventListener("autosize:update", e, !1),
                f["delete"](a),
                Object.keys(b).forEach(function(c) {
                    a.style[c] = b[c]
                })
            }
            .bind(a, {
                height: a.style.height,
                resize: a.style.resize,
                overflowY: a.style.overflowY,
                overflowX: a.style.overflowX,
                wordWrap: a.style.wordWrap
            });
            a.addEventListener("autosize:destroy", q, !1),
            "onpropertychange" in a && "oninput" in a && a.addEventListener("keyup", e, !1),
            window.addEventListener("resize", p, !1),
            a.addEventListener("input", e, !1),
            a.addEventListener("autosize:update", e, !1),
            f.add(a),
            j && (a.style.overflowX = "hidden",
            a.style.wordWrap = "break-word"),
            b()
        }
    }
    function d(a) {
        if (a && a.nodeName && "TEXTAREA" === a.nodeName) {
            var b = g("autosize:destroy");
            a.dispatchEvent(b)
        }
    }
    function e(a) {
        if (a && a.nodeName && "TEXTAREA" === a.nodeName) {
            var b = g("autosize:update");
            a.dispatchEvent(b)
        }
    }
    var f = "function" == typeof Set ? new Set : function() {
        var a = [];
        return {
            has: function(b) {
                return Boolean(a.indexOf(b) > -1)
            },
            add: function(b) {
                a.push(b)
            },
            "delete": function(b) {
                a.splice(a.indexOf(b), 1)
            }
        }
    }()
      , g = function(a) {
        return new Event(a)
    }
    ;
    try {
        new Event("test")
    } catch (h) {
        g = function(a) {
            var b = document.createEvent("Event");
            return b.initEvent(a, !0, !1),
            b
        }
    }
    var i = null ;
    "undefined" == typeof window || "function" != typeof window.getComputedStyle ? (i = function(a) {
        return a
    }
    ,
    i.destroy = function(a) {
        return a
    }
    ,
    i.update = function(a) {
        return a
    }
    ) : (i = function(a, b) {
        return a && Array.prototype.forEach.call(a.length ? a : [a], function(a) {
            return c(a, b)
        }),
        a
    }
    ,
    i.destroy = function(a) {
        return a && Array.prototype.forEach.call(a.length ? a : [a], d),
        a
    }
    ,
    i.update = function(a) {
        return a && Array.prototype.forEach.call(a.length ? a : [a], e),
        a
    }
    ),
    b.exports = i
}),
function() {
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
function(a, b) {
    "$:nomunge";
    var c, d = a.jQuery || a.Cowboy || (a.Cowboy = {});
    d.throttle = c = function(a, c, e, f) {
        function g() {
            function d() {
                i = +new Date,
                e.apply(j, l)
            }
            function g() {
                h = b
            }
            var j = this
              , k = +new Date - i
              , l = arguments;
            f && !h && d(),
            h && clearTimeout(h),
            f === b && k > a ? d() : c !== !0 && (h = setTimeout(f ? g : d, f === b ? a - k : a))
        }
        var h, i = 0;
        return "boolean" != typeof c && (f = e,
        e = c,
        c = b),
        d.guid && (g.guid = e.guid = e.guid || d.guid++),
        g
    }
    ,
    d.debounce = function(a, d, e) {
        return e === b ? c(a, d, !1) : c(a, e, d !== !1)
    }
}(this),
function(a) {
    a.fn.dfilter = function(b, c) {
        return b = b.toLowerCase(),
        b = b.replace(/^data-/, ""),
        b = b.replace("_", "-"),
        b = b.replace(" ", "-"),
        "undefined" == typeof c || "" === c ? this.filter("[data-" + b + "]") : "boolean" == typeof c ? c ? this.filter("[data-" + b + "]") : this.not("[data-" + b + "]") : a.isArray(c) ? this.filter("[data-" + b + '="' + c.join('"],[data-' + b + '="') + '"]') : this.filter("[data-" + b + '="' + c + '"]')
    }
}(jQuery),
function() {
    var a, b, c, d, e, f, g, h, i, j, k, l, m, n, o, p, q, r, s, t = [].slice, u = [].indexOf || function(a) {
        for (var b = 0, c = this.length; c > b; b++)
            if (b in this && this[b] === a)
                return b;
        return -1
    }
    ;
    a = jQuery,
    a.payment = {},
    a.payment.fn = {},
    a.fn.payment = function() {
        var b, c;
        return c = arguments[0],
        b = 2 <= arguments.length ? t.call(arguments, 1) : [],
        a.payment.fn[c].apply(this, b)
    }
    ,
    e = /(\d{1,4})/g,
    d = [{
        type: "maestro",
        pattern: /^(5018|5020|5038|6304|6759|676[1-3])/,
        format: e,
        length: [12, 13, 14, 15, 16, 17, 18, 19],
        cvcLength: [3],
        luhn: !0
    }, {
        type: "dinersclub",
        pattern: /^(36|38|30[0-5])/,
        format: e,
        length: [14],
        cvcLength: [3],
        luhn: !0
    }, {
        type: "laser",
        pattern: /^(6706|6771|6709)/,
        format: e,
        length: [16, 17, 18, 19],
        cvcLength: [3],
        luhn: !0
    }, {
        type: "jcb",
        pattern: /^35/,
        format: e,
        length: [16],
        cvcLength: [3],
        luhn: !0
    }, {
        type: "unionpay",
        pattern: /^62/,
        format: e,
        length: [16, 17, 18, 19],
        cvcLength: [3],
        luhn: !1
    }, {
        type: "discover",
        pattern: /^(6011|65|64[4-9]|622)/,
        format: e,
        length: [16],
        cvcLength: [3],
        luhn: !0
    }, {
        type: "mastercard",
        pattern: /^5[1-5]/,
        format: e,
        length: [16],
        cvcLength: [3],
        luhn: !0
    }, {
        type: "amex",
        pattern: /^3[47]/,
        format: /(\d{1,4})(\d{1,6})?(\d{1,5})?/,
        length: [15],
        cvcLength: [3, 4],
        luhn: !0
    }, {
        type: "visa",
        pattern: /^4/,
        format: e,
        length: [13, 16],
        cvcLength: [3],
        luhn: !0
    }],
    b = function(a) {
        var b, c, e;
        for (a = (a + "").replace(/\D/g, ""),
        c = 0,
        e = d.length; e > c; c++)
            if (b = d[c],
            b.pattern.test(a))
                return b
    }
    ,
    c = function(a) {
        var b, c, e;
        for (c = 0,
        e = d.length; e > c; c++)
            if (b = d[c],
            b.type === a)
                return b
    }
    ,
    m = function(a) {
        var b, c, d, e, f, g;
        for (d = !0,
        e = 0,
        c = (a + "").split("").reverse(),
        f = 0,
        g = c.length; g > f; f++)
            b = c[f],
            b = parseInt(b, 10),
            (d = !d) && (b *= 2),
            b > 9 && (b -= 9),
            e += b;
        return e % 10 === 0
    }
    ,
    l = function(a) {
        var b;
        return null  != a.prop("selectionStart") && a.prop("selectionStart") !== a.prop("selectionEnd") ? !0 : !("undefined" != typeof document && null  !== document && null  != (b = document.selection) && "function" == typeof b.createRange ? !b.createRange().text : !0)
    }
    ,
    n = function(b) {
        return setTimeout(function(c) {
            return function() {
                var c, d;
                return c = a(b.currentTarget),
                d = c.val(),
                d = a.payment.formatCardNumber(d),
                c.val(d)
            }
        }(this))
    }
    ,
    h = function(c) {
        var d, e, f, g, h, i, j;
        return f = String.fromCharCode(c.which),
        !/^\d+$/.test(f) || (d = a(c.currentTarget),
        j = d.val(),
        e = b(j + f),
        g = (j.replace(/\D/g, "") + f).length,
        i = 16,
        e && (i = e.length[e.length.length - 1]),
        g >= i || null  != d.prop("selectionStart") && d.prop("selectionStart") !== j.length) ? void 0 : (h = e && "amex" === e.type ? /^(\d{4}|\d{4}\s\d{6})$/ : /(?:^|\s)(\d{4})$/,
        h.test(j) ? (c.preventDefault(),
        d.val(j + " " + f)) : h.test(j + f) ? (c.preventDefault(),
        d.val(j + f + " ")) : void 0)
    }
    ,
    f = function(b) {
        var c, d;
        return c = a(b.currentTarget),
        d = c.val(),
        b.meta || 8 !== b.which || null  != c.prop("selectionStart") && c.prop("selectionStart") !== d.length ? void 0 : /\d\s$/.test(d) ? (b.preventDefault(),
        c.val(d.replace(/\d\s$/, ""))) : /\s\d?$/.test(d) ? (b.preventDefault(),
        c.val(d.replace(/\s\d?$/, ""))) : void 0
    }
    ,
    i = function(b) {
        var c, d, e;
        return d = String.fromCharCode(b.which),
        /^\d+$/.test(d) ? (c = a(b.currentTarget),
        e = c.val() + d,
        /^\d$/.test(e) && "0" !== e && "1" !== e ? (b.preventDefault(),
        c.val("0" + e + " / ")) : /^\d\d$/.test(e) ? (b.preventDefault(),
        c.val("" + e + " / ")) : void 0) : void 0
    }
    ,
    j = function(b) {
        var c, d, e;
        return d = String.fromCharCode(b.which),
        /^\d+$/.test(d) ? (c = a(b.currentTarget),
        e = c.val(),
        /^\d\d$/.test(e) ? c.val("" + e + " / ") : void 0) : void 0
    }
    ,
    k = function(b) {
        var c, d, e;
        return d = String.fromCharCode(b.which),
        "/" === d ? (c = a(b.currentTarget),
        e = c.val(),
        /^\d$/.test(e) && "0" !== e ? c.val("0" + e + " / ") : void 0) : void 0
    }
    ,
    g = function(b) {
        var c, d;
        if (!b.meta && (c = a(b.currentTarget),
        d = c.val(),
        8 === b.which && (null  == c.prop("selectionStart") || c.prop("selectionStart") === d.length)))
            return /\d(\s|\/)+$/.test(d) ? (b.preventDefault(),
            c.val(d.replace(/\d(\s|\/)*$/, ""))) : /\s\/\s?\d?$/.test(d) ? (b.preventDefault(),
            c.val(d.replace(/\s\/\s?\d?$/, ""))) : void 0
    }
    ,
    r = function(a) {
        var b;
        return a.metaKey || a.ctrlKey ? !0 : 32 === a.which ? !1 : 0 === a.which ? !0 : a.which < 33 ? !0 : (b = String.fromCharCode(a.which),
        !!/[\d\s]/.test(b))
    }
    ,
    p = function(c) {
        var d, e, f, g;
        return d = a(c.currentTarget),
        f = String.fromCharCode(c.which),
        /^\d+$/.test(f) && !l(d) ? (g = (d.val() + f).replace(/\D/g, ""),
        e = b(g),
        e ? g.length <= e.length[e.length.length - 1] : g.length <= 16) : void 0
    }
    ,
    q = function(b) {
        var c, d, e;
        return c = a(b.currentTarget),
        d = String.fromCharCode(b.which),
        /^\d+$/.test(d) && !l(c) ? (e = c.val() + d,
        e = e.replace(/\D/g, ""),
        e.length > 6 ? !1 : void 0) : void 0
    }
    ,
    o = function(b) {
        var c, d, e;
        return c = a(b.currentTarget),
        d = String.fromCharCode(b.which),
        /^\d+$/.test(d) && !l(c) ? (e = c.val() + d,
        e.length <= 4) : void 0
    }
    ,
    s = function(b) {
        var c, e, f, g, h;
        return c = a(b.currentTarget),
        h = c.val(),
        g = a.payment.cardType(h) || "unknown",
        c.hasClass(g) ? void 0 : (e = function() {
            var a, b, c;
            for (c = [],
            a = 0,
            b = d.length; b > a; a++)
                f = d[a],
                c.push(f.type);
            return c
        }(),
        c.removeClass("unknown"),
        c.removeClass(e.join(" ")),
        c.addClass(g),
        c.toggleClass("identified", "unknown" !== g),
        c.trigger("payment.cardType", g))
    }
    ,
    a.payment.fn.formatCardCVC = function() {
        return this.payment("restrictNumeric"),
        this.on("keypress", o),
        this
    }
    ,
    a.payment.fn.formatCardExpiry = function() {
        return this.payment("restrictNumeric"),
        this.on("keypress", q),
        this.on("keypress", i),
        this.on("keypress", k),
        this.on("keypress", j),
        this.on("keydown", g),
        this
    }
    ,
    a.payment.fn.formatCardNumber = function() {
        return this.payment("restrictNumeric"),
        this.on("keypress", p),
        this.on("keypress", h),
        this.on("keydown", f),
        this.on("keyup", s),
        this.on("paste", n),
        this
    }
    ,
    a.payment.fn.restrictNumeric = function() {
        return this.on("keypress", r),
        this
    }
    ,
    a.payment.fn.cardExpiryVal = function() {
        return a.payment.cardExpiryVal(a(this).val())
    }
    ,
    a.payment.cardExpiryVal = function(a) {
        var b, c, d, e;
        return a = a.replace(/\s/g, ""),
        e = a.split("/", 2),
        b = e[0],
        d = e[1],
        2 === (null  != d ? d.length : void 0) && /^\d+$/.test(d) && (c = (new Date).getFullYear(),
        c = c.toString().slice(0, 2),
        d = c + d),
        b = parseInt(b, 10),
        d = parseInt(d, 10),
        {
            month: b,
            year: d
        }
    }
    ,
    a.payment.validateCardNumber = function(a) {
        var c, d;
        return a = (a + "").replace(/\s+|-/g, ""),
        /^\d+$/.test(a) ? (c = b(a),
        c ? (d = a.length,
        u.call(c.length, d) >= 0 && (c.luhn === !1 || m(a))) : !1) : !1
    }
    ,
    a.payment.validateCardExpiry = function(b) {
        return function(b, c) {
            var d, e, f, g;
            return "object" == typeof b && "month" in b && (g = b,
            b = g.month,
            c = g.year),
            b && c ? (b = a.trim(b),
            c = a.trim(c),
            /^\d+$/.test(b) && /^\d+$/.test(c) && parseInt(b, 10) <= 12 ? (2 === c.length && (f = (new Date).getFullYear(),
            f = f.toString().slice(0, 2),
            c = f + c),
            e = new Date(c,b),
            d = new Date,
            e.setMonth(e.getMonth() - 1),
            e.setMonth(e.getMonth() + 1, 1),
            e > d) : !1) : !1
        }
    }(this),
    a.payment.validateCardCVC = function(b, d) {
        var e, f;
        return b = a.trim(b),
        /^\d+$/.test(b) ? d ? (e = b.length,
        u.call(null  != (f = c(d)) ? f.cvcLength : void 0, e) >= 0) : b.length >= 3 && b.length <= 4 : !1
    }
    ,
    a.payment.cardType = function(a) {
        var c;
        return a ? (null  != (c = b(a)) ? c.type : void 0) || null  : null 
    }
    ,
    a.payment.formatCardNumber = function(a) {
        var c, d, e, f;
        return (c = b(a)) ? (e = c.length[c.length.length - 1],
        a = a.replace(/\D/g, ""),
        a = a.slice(0, +e + 1 || 9e9),
        c.format.global ? null  != (f = a.match(c.format)) ? f.join(" ") : void 0 : (d = c.format.exec(a),
        null  != d && d.shift(),
        null  != d ? d.join(" ") : void 0)) : a
    }
}
.call(this),
function(a, b, c) {
    function d(a, b, c) {
        return a.addEventListener ? void a.addEventListener(b, c, !1) : void a.attachEvent("on" + b, c)
    }
    function e(a) {
        if ("keypress" == a.type) {
            var b = String.fromCharCode(a.which);
            return a.shiftKey || (b = b.toLowerCase()),
            b
        }
        return x[a.which] ? x[a.which] : y[a.which] ? y[a.which] : String.fromCharCode(a.which).toLowerCase()
    }
    function f(a, b) {
        return a.sort().join(",") === b.sort().join(",")
    }
    function g(a) {
        a = a || {};
        var b, c = !1;
        for (b in D)
            a[b] ? c = !0 : D[b] = 0;
        c || (G = !1)
    }
    function h(a, b, c, d, e, g) {
        var h, i, j = [], k = c.type;
        if (!B[a])
            return [];
        for ("keyup" == k && m(a) && (b = [a]),
        h = 0; h < B[a].length; ++h)
            if (i = B[a][h],
            (d || !i.seq || D[i.seq] == i.level) && k == i.action && ("keypress" == k && !c.metaKey && !c.ctrlKey || f(b, i.modifiers))) {
                var l = !d && i.combo == e
                  , n = d && i.seq == d && i.level == g;
                (l || n) && B[a].splice(h, 1),
                j.push(i)
            }
        return j
    }
    function i(a) {
        var b = [];
        return a.shiftKey && b.push("shift"),
        a.altKey && b.push("alt"),
        a.ctrlKey && b.push("ctrl"),
        a.metaKey && b.push("meta"),
        b
    }
    function j(a, b, c, d) {
        I.stopCallback(b, b.target || b.srcElement, c, d) || a(b, c) === !1 && (b.preventDefault && b.preventDefault(),
        b.stopPropagation && b.stopPropagation(),
        b.returnValue = !1,
        b.cancelBubble = !0)
    }
    function k(a, b, c) {
        var d, e = h(a, b, c), f = {}, i = 0, k = !1;
        for (d = 0; d < e.length; ++d)
            e[d].seq && (i = Math.max(i, e[d].level));
        for (d = 0; d < e.length; ++d)
            if (e[d].seq) {
                if (e[d].level != i)
                    continue;k = !0,
                f[e[d].seq] = 1,
                j(e[d].callback, c, e[d].combo, e[d].seq)
            } else
                k || j(e[d].callback, c, e[d].combo);
        var l = "keypress" == c.type && F;
        c.type != G || m(a) || l || g(f),
        F = k && "keydown" == c.type
    }
    function l(a) {
        "number" != typeof a.which && (a.which = a.keyCode);
        var b = e(a);
        if (b)
            return "keyup" == a.type && E === b ? void (E = !1) : void I.handleKey(b, i(a), a)
    }
    function m(a) {
        return "shift" == a || "ctrl" == a || "alt" == a || "meta" == a
    }
    function n() {
        clearTimeout(w),
        w = setTimeout(g, 1e3)
    }
    function o() {
        if (!v) {
            v = {};
            for (var a in x)
                a > 95 && 112 > a || x.hasOwnProperty(a) && (v[x[a]] = a)
        }
        return v
    }
    function p(a, b, c) {
        return c || (c = o()[a] ? "keydown" : "keypress"),
        "keypress" == c && b.length && (c = "keydown"),
        c
    }
    function q(a, b, c, d) {
        function f(b) {
            return function() {
                G = b,
                ++D[a],
                n()
            }
        }
        function h(b) {
            j(c, b, a),
            "keyup" !== d && (E = e(b)),
            setTimeout(g, 10)
        }
        D[a] = 0;
        for (var i = 0; i < b.length; ++i) {
            var k = i + 1 === b.length
              , l = k ? h : f(d || s(b[i + 1]).action);
            t(b[i], l, d, a, i)
        }
    }
    function r(a) {
        return "+" === a ? ["+"] : a.split("+")
    }
    function s(a, b) {
        var c, d, e, f = [];
        for (c = r(a),
        e = 0; e < c.length; ++e)
            d = c[e],
            A[d] && (d = A[d]),
            b && "keypress" != b && z[d] && (d = z[d],
            f.push("shift")),
            m(d) && f.push(d);
        return b = p(d, f, b),
        {
            key: d,
            modifiers: f,
            action: b
        }
    }
    function t(a, b, c, d, e) {
        C[a + ":" + c] = b,
        a = a.replace(/\s+/g, " ");
        var f, g = a.split(" ");
        return g.length > 1 ? void q(a, g, b, c) : (f = s(a, c),
        B[f.key] = B[f.key] || [],
        h(f.key, f.modifiers, {
            type: f.action
        }, d, a, e),
        void B[f.key][d ? "unshift" : "push"]({
            callback: b,
            modifiers: f.modifiers,
            action: f.action,
            seq: d,
            level: e,
            combo: a
        }))
    }
    function u(a, b, c) {
        for (var d = 0; d < a.length; ++d)
            t(a[d], b, c)
    }
    for (var v, w, x = {
        8: "backspace",
        9: "tab",
        13: "enter",
        16: "shift",
        17: "ctrl",
        18: "alt",
        20: "capslock",
        27: "esc",
        32: "space",
        33: "pageup",
        34: "pagedown",
        35: "end",
        36: "home",
        37: "left",
        38: "up",
        39: "right",
        40: "down",
        45: "ins",
        46: "del",
        91: "meta",
        93: "meta",
        224: "meta"
    }, y = {
        106: "*",
        107: "+",
        109: "-",
        110: ".",
        111: "/",
        186: ";",
        187: "=",
        188: ",",
        189: "-",
        190: ".",
        191: "/",
        192: "`",
        219: "[",
        220: "\\",
        221: "]",
        222: "'"
    }, z = {
        "~": "`",
        "!": "1",
        "@": "2",
        "#": "3",
        $: "4",
        "%": "5",
        "^": "6",
        "&": "7",
        "*": "8",
        "(": "9",
        ")": "0",
        _: "-",
        "+": "=",
        ":": ";",
        '"': "'",
        "<": ",",
        ">": ".",
        "?": "/",
        "|": "\\"
    }, A = {
        option: "alt",
        command: "meta",
        "return": "enter",
        escape: "esc",
        mod: /Mac|iPod|iPhone|iPad/.test(navigator.platform) ? "meta" : "ctrl"
    }, B = {}, C = {}, D = {}, E = !1, F = !1, G = !1, H = 1; 20 > H; ++H)
        x[111 + H] = "f" + H;
    for (H = 0; 9 >= H; ++H)
        x[H + 96] = H;
    d(b, "keypress", l),
    d(b, "keydown", l),
    d(b, "keyup", l);
    var I = {
        bind: function(a, b, c) {
            return a = a instanceof Array ? a : [a],
            u(a, b, c),
            this
        },
        unbind: function(a, b) {
            return I.bind(a, function() {}, b)
        },
        trigger: function(a, b) {
            return C[a + ":" + b] && C[a + ":" + b]({}, a),
            this
        },
        reset: function() {
            return B = {},
            C = {},
            this
        },
        stopCallback: function(a, b) {
            return (" " + b.className + " ").indexOf(" mousetrap ") > -1 ? !1 : "INPUT" == b.tagName || "SELECT" == b.tagName || "TEXTAREA" == b.tagName || b.isContentEditable
        },
        handleKey: k
    };
    a.Mousetrap = I,
    "function" == typeof define && define.amd && define(I)
}(window, document),
function(a, b) {
    var c = function(a) {
        var b = a.url.replace(/jQuery.*/, "");
        return a.cache === !1 && (b = b.replace(/([?&])_=[^&]*/, "")),
        a.cacheKey || b + a.type + (a.data || "")
    }
      , d = function(a) {
        if (!a)
            return !1;
        if (a === !0)
            return b.localStorage;
        if ("object" == typeof a && "getItem" in a && "removeItem" in a && "setItem" in a)
            return a;
        throw new TypeError("localCache must either be a boolean value, or an object which implements the Storage interface.")
    }
    ;
    a.ajaxPrefilter(function(a) {
        var b, e, f = d(a.localCache), g = a.cacheTTL || 5, h = c(a), i = a.isCacheValid;
        f && (b = f.getItem(h + "cachettl"),
        i && "function" == typeof i && !i() && f.removeItem(h),
        b && b < +new Date && (f.removeItem(h),
        f.removeItem(h + "cachettl"),
        b = 0),
        e = f.getItem(h),
        e || (a.success && (a.realsuccess = a.success),
        a.success = function(b) {
            var c = b;
            0 === this.dataType.toLowerCase().indexOf("json") && (c = JSON.stringify(b));
            try {
                f.setItem(h, c)
            } catch (d) {
                f.removeItem(h),
                f.removeItem(h + "cachettl"),
                console.log("Cache Error:" + d, h, c)
            }
            a.realsuccess && a.realsuccess(b)
        }
        ,
        b || f.setItem(h + "cachettl", +new Date + 36e5 * g)))
    }),
    a.ajaxTransport("+*", function(a) {
        if (a.localCache) {
            var b = c(a)
              , e = d(a.localCache)
              , f = e ? e.getItem(b) : !1;
            if (f)
                return 0 === a.dataType.toLowerCase().indexOf("json") && (f = JSON.parse(f)),
                {
                    send: function(b, c) {
                        var d = {};
                        d[a.dataType] = f,
                        c(200, "success", d, "")
                    },
                    abort: function() {
                        console.log("Aborted ajax transport for json cache.")
                    }
                }
        }
    })
}(jQuery, window),
function(a) {
    "function" == typeof define && define.amd ? define(["jquery"], a) : "object" == typeof module && "object" == typeof module.exports ? module.exports = a(require("jquery")) : a(jQuery)
}(function(a) {
    "use strict";
    a.extend({
        tablesorter: new function() {
            function b() {
                var a = arguments[0]
                  , b = arguments.length > 1 ? Array.prototype.slice.call(arguments) : a;
                "undefined" != typeof console && "undefined" != typeof console.log ? console[/error/i.test(a) ? "error" : /warn/i.test(a) ? "warn" : "log"](b) : alert(b)
            }
            function c(a, c) {
                b(a + " (" + ((new Date).getTime() - c.getTime()) + "ms)")
            }
            function d(a) {
                for (var b in a)
                    return !1;
                return !0
            }
            function e(c, d, e, f) {
                for (var g, h, i = c.config, j = u.parsers.length, k = !1, l = "", m = !0; "" === l && m; )
                    e++,
                    d[e] ? (k = d[e].cells[f],
                    l = u.getElementText(i, k, f),
                    h = a(k),
                    c.config.debug && b("Checking if value was empty on row " + e + ", column: " + f + ': "' + l + '"')) : m = !1;
                for (; --j >= 0; )
                    if (g = u.parsers[j],
                    g && "text" !== g.id && g.is && g.is(l, c, k, h))
                        return g;
                return u.getParserById("text")
            }
            function f(a) {
                var d, f, g, h, i, j, k, l, m, n, o = a.config, p = o.$tbodies = o.$table.children("tbody:not(." + o.cssInfoBlock + ")"), q = 0, r = "", s = p.length;
                if (0 === s)
                    return o.debug ? b("Warning: *Empty table!* Not building a parser cache") : "";
                for (o.debug && (n = new Date,
                b("Detecting parsers for each column")),
                f = {
                    extractors: [],
                    parsers: []
                }; s > q; ) {
                    if (d = p[q].rows,
                    d.length)
                        for (g = o.columns,
                        h = 0; g > h; h++)
                            i = o.$headerIndexed[h],
                            j = u.getColumnData(a, o.headers, h),
                            m = u.getParserById(u.getData(i, j, "extractor")),
                            l = u.getParserById(u.getData(i, j, "sorter")),
                            k = "false" === u.getData(i, j, "parser"),
                            o.empties[h] = (u.getData(i, j, "empty") || o.emptyTo || (o.emptyToBottom ? "bottom" : "top")).toLowerCase(),
                            o.strings[h] = (u.getData(i, j, "string") || o.stringTo || "max").toLowerCase(),
                            k && (l = u.getParserById("no-parser")),
                            m || (m = !1),
                            l || (l = e(a, d, -1, h)),
                            o.debug && (r += "column:" + h + "; extractor:" + m.id + "; parser:" + l.id + "; string:" + o.strings[h] + "; empty: " + o.empties[h] + "\n"),
                            f.parsers[h] = l,
                            f.extractors[h] = m;
                    q += f.parsers.length ? s : 1
                }
                o.debug && (b(r ? r : "No parsers detected"),
                c("Completed detecting parsers", n)),
                o.parsers = f.parsers,
                o.extractors = f.extractors
            }
            function g(d) {
                var e, f, g, h, i, j, k, l, m, n, o, p, q, r = d.config, s = r.$tbodies, t = r.extractors, v = r.parsers;
                if (r.cache = {},
                r.totalRows = 0,
                !v)
                    return r.debug ? b("Warning: *Empty table!* Not building a cache") : "";
                for (r.debug && (n = new Date),
                r.showProcessing && u.isProcessing(d, !0),
                k = 0; k < s.length; k++) {
                    for (q = [],
                    e = r.cache[k] = {
                        normalized: []
                    },
                    o = s[k] && s[k].rows.length || 0,
                    i = 0; o > i; ++i)
                        if (p = {
                            child: [],
                            raw: []
                        },
                        l = a(s[k].rows[i]),
                        m = [],
                        l.hasClass(r.cssChildRow) && 0 !== i)
                            f = e.normalized.length - 1,
                            e.normalized[f][r.columns].$row = e.normalized[f][r.columns].$row.add(l),
                            l.prev().hasClass(r.cssChildRow) || l.prev().addClass(u.css.cssHasChild),
                            p.child[f] = a.trim(l[0].textContent || l.text() || "");
                        else {
                            for (p.$row = l,
                            p.order = i,
                            j = 0; j < r.columns; ++j)
                                "undefined" != typeof v[j] ? (f = u.getElementText(r, l[0].cells[j], j),
                                p.raw.push(f),
                                g = "undefined" == typeof t[j].id ? f : t[j].format(f, d, l[0].cells[j], j),
                                h = "no-parser" === v[j].id ? "" : v[j].format(g, d, l[0].cells[j], j),
                                m.push(r.ignoreCase && "string" == typeof h ? h.toLowerCase() : h),
                                "numeric" === (v[j].type || "").toLowerCase() && (q[j] = Math.max(Math.abs(h) || 0, q[j] || 0))) : r.debug && b("No parser found for cell:", l[0].cells[j], "does it have a header?");
                            m[r.columns] = p,
                            e.normalized.push(m)
                        }
                    e.colMax = q,
                    r.totalRows += e.normalized.length
                }
                r.showProcessing && u.isProcessing(d),
                r.debug && c("Building cache for " + o + " rows", n)
            }
            function h(a, b) {
                var e, f, g, h, i, j, k, l = a.config, m = l.widgetOptions, n = l.$tbodies, o = [], p = l.cache;
                if (d(p))
                    return l.appender ? l.appender(a, o) : a.isUpdating ? l.$table.trigger("updateComplete", a) : "";
                for (l.debug && (k = new Date),
                j = 0; j < n.length; j++)
                    if (g = n.eq(j),
                    g.length) {
                        for (h = u.processTbody(a, g, !0),
                        e = p[j].normalized,
                        f = e.length,
                        i = 0; f > i; i++)
                            o.push(e[i][l.columns].$row),
                            l.appender && (!l.pager || l.pager.removeRows && m.pager_removeRows || l.pager.ajax) || h.append(e[i][l.columns].$row);
                        u.processTbody(a, h, !1)
                    }
                l.appender && l.appender(a, o),
                l.debug && c("Rebuilt table", k),
                b || l.appender || u.applyWidget(a),
                a.isUpdating && l.$table.trigger("updateComplete", a)
            }
            function i(a) {
                return /^d/i.test(a) || 1 === a
            }
            function j(d) {
                var e, f, g, h, j, k, m, n, o = d.config;
                for (o.headerList = [],
                o.headerContent = [],
                o.debug && (m = new Date),
                o.columns = u.computeColumnIndex(o.$table.children("thead, tfoot").children("tr")),
                h = o.cssIcon ? '<i class="' + (o.cssIcon === u.css.icon ? u.css.icon : o.cssIcon + " " + u.css.icon) + '"></i>' : "",
                o.$headers = a(a.map(a(d).find(o.selectorHeaders), function(b, c) {
                    return f = a(b),
                    f.parent().hasClass(o.cssIgnoreRow) ? void 0 : (e = u.getColumnData(d, o.headers, c, !0),
                    o.headerContent[c] = f.html(),
                    "" === o.headerTemplate || f.find("." + u.css.headerIn).length || (j = o.headerTemplate.replace(/\{content\}/g, f.html()).replace(/\{icon\}/g, f.find("." + u.css.icon).length ? "" : h),
                    o.onRenderTemplate && (g = o.onRenderTemplate.apply(f, [c, j]),
                    g && "string" == typeof g && (j = g)),
                    f.html('<div class="' + u.css.headerIn + '">' + j + "</div>")),
                    o.onRenderHeader && o.onRenderHeader.apply(f, [c, o, o.$table]),
                    b.column = parseInt(f.attr("data-column"), 10),
                    b.order = i(u.getData(f, e, "sortInitialOrder") || o.sortInitialOrder) ? [1, 0, 2] : [0, 1, 2],
                    b.count = -1,
                    b.lockedOrder = !1,
                    k = u.getData(f, e, "lockedOrder") || !1,
                    "undefined" != typeof k && k !== !1 && (b.order = b.lockedOrder = i(k) ? [1, 1, 1] : [0, 0, 0]),
                    f.addClass(u.css.header + " " + o.cssHeader),
                    o.headerList[c] = b,
                    f.parent().addClass(u.css.headerRow + " " + o.cssHeaderRow).attr("role", "row"),
                    o.tabIndex && f.attr("tabindex", 0),
                    b)
                })),
                o.$headerIndexed = [],
                n = 0; n < o.columns; n++)
                    f = o.$headers.filter('[data-column="' + n + '"]'),
                    o.$headerIndexed[n] = f.not(".sorter-false").length ? f.not(".sorter-false").filter(":last") : f.filter(":last");
                a(d).find(o.selectorHeaders).attr({
                    scope: "col",
                    role: "columnheader"
                }),
                l(d),
                o.debug && (c("Built headers:", m),
                b(o.$headers))
            }
            function k(a, b, c) {
                var d = a.config;
                d.$table.find(d.selectorRemove).remove(),
                f(a),
                g(a),
                s(d, b, c)
            }
            function l(b) {
                var c, d, e, f = b.config;
                f.$headers.each(function(g, h) {
                    d = a(h),
                    e = u.getColumnData(b, f.headers, g, !0),
                    c = "false" === u.getData(h, e, "sorter") || "false" === u.getData(h, e, "parser"),
                    h.sortDisabled = c,
                    d[c ? "addClass" : "removeClass"]("sorter-false").attr("aria-disabled", "" + c),
                    b.id && (c ? d.removeAttr("aria-controls") : d.attr("aria-controls", b.id))
                })
            }
            function m(b) {
                var c, d, e, f = b.config, g = f.sortList, h = g.length, i = u.css.sortNone + " " + f.cssNone, j = [u.css.sortAsc + " " + f.cssAsc, u.css.sortDesc + " " + f.cssDesc], k = [f.cssIconAsc, f.cssIconDesc, f.cssIconNone], l = ["ascending", "descending"], m = a(b).find("tfoot tr").children().add(f.$extraHeaders).removeClass(j.join(" "));
                for (f.$headers.removeClass(j.join(" ")).addClass(i).attr("aria-sort", "none").find("." + u.css.icon).removeClass(k.join(" ")).addClass(k[2]),
                d = 0; h > d; d++)
                    if (2 !== g[d][1] && (c = f.$headers.not(".sorter-false").filter('[data-column="' + g[d][0] + '"]' + (1 === h ? ":last" : "")),
                    c.length)) {
                        for (e = 0; e < c.length; e++)
                            c[e].sortDisabled || c.eq(e).removeClass(i).addClass(j[g[d][1]]).attr("aria-sort", l[g[d][1]]).find("." + u.css.icon).removeClass(k[2]).addClass(k[g[d][1]]);
                        m.length && m.filter('[data-column="' + g[d][0] + '"]').removeClass(i).addClass(j[g[d][1]])
                    }
                f.$headers.not(".sorter-false").each(function() {
                    var b = a(this)
                      , c = this.order[(this.count + 1) % (f.sortReset ? 3 : 2)]
                      , d = a.trim(b.text()) + ": " + u.language[b.hasClass(u.css.sortAsc) ? "sortAsc" : b.hasClass(u.css.sortDesc) ? "sortDesc" : "sortNone"] + u.language[0 === c ? "nextAsc" : 1 === c ? "nextDesc" : "nextNone"];
                    b.attr("aria-label", d)
                })
            }
            function n(b, c) {
                var d, e, f, g, h, i, j, k, l = b.config, m = c || l.sortList, n = m.length;
                for (l.sortList = [],
                h = 0; n > h; h++)
                    if (k = m[h],
                    d = parseInt(k[0], 10),
                    g = l.$headerIndexed[d][0]) {
                        switch (e = ("" + k[1]).match(/^(1|d|s|o|n)/),
                        e = e ? e[0] : "") {
                        case "1":
                        case "d":
                            e = 1;
                            break;
                        case "s":
                            e = i || 0;
                            break;
                        case "o":
                            j = g.order[(i || 0) % (l.sortReset ? 3 : 2)],
                            e = 0 === j ? 1 : 1 === j ? 0 : 2;
                            break;
                        case "n":
                            g.count = g.count + 1,
                            e = g.order[g.count % (l.sortReset ? 3 : 2)];
                            break;
                        default:
                            e = 0
                        }
                        i = 0 === h ? e : i,
                        f = [d, parseInt(e, 10) || 0],
                        l.sortList.push(f),
                        e = a.inArray(f[1], g.order),
                        g.count = e >= 0 ? e : f[1] % (l.sortReset ? 3 : 2)
                    }
            }
            function o(a, b) {
                return a && a[b] ? a[b].type || "" : ""
            }
            function p(b, c, d) {
                if (b.isUpdating)
                    return setTimeout(function() {
                        p(b, c, d)
                    }, 50);
                var e, f, g, i, j, k = b.config, l = !d[k.sortMultiSortKey], n = k.$table;
                if (n.trigger("sortStart", b),
                c.count = d[k.sortResetKey] ? 2 : (c.count + 1) % (k.sortReset ? 3 : 2),
                k.sortRestart && (f = c,
                k.$headers.each(function() {
                    this === f || !l && a(this).is("." + u.css.sortDesc + ",." + u.css.sortAsc) || (this.count = -1)
                })),
                f = parseInt(a(c).attr("data-column"), 10),
                l) {
                    if (k.sortList = [],
                    null  !== k.sortForce)
                        for (e = k.sortForce,
                        g = 0; g < e.length; g++)
                            e[g][0] !== f && k.sortList.push(e[g]);
                    if (i = c.order[c.count],
                    2 > i && (k.sortList.push([f, i]),
                    c.colSpan > 1))
                        for (g = 1; g < c.colSpan; g++)
                            k.sortList.push([f + g, i])
                } else {
                    if (k.sortAppend && k.sortList.length > 1)
                        for (g = 0; g < k.sortAppend.length; g++)
                            j = u.isValueInArray(k.sortAppend[g][0], k.sortList),
                            j >= 0 && k.sortList.splice(j, 1);
                    if (u.isValueInArray(f, k.sortList) >= 0)
                        for (g = 0; g < k.sortList.length; g++)
                            j = k.sortList[g],
                            i = k.$headerIndexed[j[0]][0],
                            j[0] === f && (j[1] = i.order[c.count],
                            2 === j[1] && (k.sortList.splice(g, 1),
                            i.count = -1));
                    else if (i = c.order[c.count],
                    2 > i && (k.sortList.push([f, i]),
                    c.colSpan > 1))
                        for (g = 1; g < c.colSpan; g++)
                            k.sortList.push([f + g, i])
                }
                if (null  !== k.sortAppend)
                    for (e = k.sortAppend,
                    g = 0; g < e.length; g++)
                        e[g][0] !== f && k.sortList.push(e[g]);
                n.trigger("sortBegin", b),
                setTimeout(function() {
                    m(b),
                    q(b),
                    h(b),
                    n.trigger("sortEnd", b)
                }, 1)
            }
            function q(a) {
                var b, e, f, g, h, i, j, k, l, m, n, p = 0, q = a.config, r = q.textSorter || "", s = q.sortList, t = s.length, v = q.$tbodies.length;
                if (!q.serverSideSorting && !d(q.cache)) {
                    for (q.debug && (h = new Date),
                    e = 0; v > e; e++)
                        i = q.cache[e].colMax,
                        j = q.cache[e].normalized,
                        j.sort(function(c, d) {
                            for (b = 0; t > b; b++) {
                                if (g = s[b][0],
                                k = s[b][1],
                                p = 0 === k,
                                q.sortStable && c[g] === d[g] && 1 === t)
                                    return c[q.columns].order - d[q.columns].order;
                                if (f = /n/i.test(o(q.parsers, g)),
                                f && q.strings[g] ? (f = "boolean" == typeof q.string[q.strings[g]] ? (p ? 1 : -1) * (q.string[q.strings[g]] ? -1 : 1) : q.strings[g] ? q.string[q.strings[g]] || 0 : 0,
                                l = q.numberSorter ? q.numberSorter(c[g], d[g], p, i[g], a) : u["sortNumeric" + (p ? "Asc" : "Desc")](c[g], d[g], f, i[g], g, a)) : (m = p ? c : d,
                                n = p ? d : c,
                                l = "function" == typeof r ? r(m[g], n[g], p, g, a) : "object" == typeof r && r.hasOwnProperty(g) ? r[g](m[g], n[g], p, g, a) : u["sortNatural" + (p ? "Asc" : "Desc")](c[g], d[g], g, a, q)),
                                l)
                                    return l
                            }
                            return c[q.columns].order - d[q.columns].order
                        });
                    q.debug && c("Sorting on " + s.toString() + " and dir " + k + " time", h)
                }
            }
            function r(b, c) {
                b.table.isUpdating && b.$table.trigger("updateComplete", b.table),
                a.isFunction(c) && c(b.table)
            }
            function s(b, c, d) {
                var e = a.isArray(c) ? c : b.sortList
                  , f = "undefined" == typeof c ? b.resort : c;
                f === !1 || b.serverSideSorting || b.table.isProcessing ? (r(b, d),
                u.applyWidget(b.table, !1)) : e.length ? b.$table.trigger("sorton", [e, function() {
                    r(b, d)
                }
                , !0]) : b.$table.trigger("sortReset", [function() {
                    r(b, d),
                    u.applyWidget(b.table, !1)
                }
                ])
            }
            function t(b) {
                var c = b.config
                  , e = c.$table
                  , i = "sortReset update updateRows updateCell updateAll addRows updateComplete sorton appendCache updateCache applyWidgetId applyWidgets refreshWidgets destroy mouseup mouseleave ".split(" ").join(c.namespace + " ");
                e.unbind(i.replace(/\s+/g, " ")).bind("sortReset" + c.namespace, function(d, e) {
                    d.stopPropagation(),
                    c.sortList = [],
                    m(b),
                    q(b),
                    h(b),
                    a.isFunction(e) && e(b)
                }).bind("updateAll" + c.namespace, function(a, d, e) {
                    a.stopPropagation(),
                    b.isUpdating = !0,
                    u.refreshWidgets(b, !0, !0),
                    j(b),
                    u.bindEvents(b, c.$headers, !0),
                    t(b),
                    k(b, d, e)
                }).bind("update" + c.namespace + " updateRows" + c.namespace, function(a, c, d) {
                    a.stopPropagation(),
                    b.isUpdating = !0,
                    l(b),
                    k(b, c, d)
                }).bind("updateCell" + c.namespace, function(d, f, g, h) {
                    d.stopPropagation(),
                    b.isUpdating = !0,
                    e.find(c.selectorRemove).remove();
                    var i, j, k, l, m = c.$tbodies, n = a(f), o = m.index(a.fn.closest ? n.closest("tbody") : n.parents("tbody").filter(":first")), p = a.fn.closest ? n.closest("tr") : n.parents("tr").filter(":first");
                    f = n[0],
                    m.length && o >= 0 && (k = m.eq(o).find("tr").index(p),
                    l = n.index(),
                    c.cache[o].normalized[k][c.columns].$row = p,
                    j = "undefined" == typeof c.extractors[l].id ? u.getElementText(c, f, l) : c.extractors[l].format(u.getElementText(c, f, l), b, f, l),
                    i = "no-parser" === c.parsers[l].id ? "" : c.parsers[l].format(j, b, f, l),
                    c.cache[o].normalized[k][l] = c.ignoreCase && "string" == typeof i ? i.toLowerCase() : i,
                    "numeric" === (c.parsers[l].type || "").toLowerCase() && (c.cache[o].colMax[l] = Math.max(Math.abs(i) || 0, c.cache[o].colMax[l] || 0)),
                    i = "undefined" !== g ? g : c.resort,
                    i !== !1 ? s(c, i, h) : (a.isFunction(h) && h(b),
                    c.$table.trigger("updateComplete", c.table)))
                }).bind("addRows" + c.namespace, function(e, g, h, i) {
                    if (e.stopPropagation(),
                    b.isUpdating = !0,
                    d(c.cache))
                        l(b),
                        k(b, h, i);
                    else {
                        g = a(g).attr("role", "row");
                        var j, m, n, o, p, q, r, t = g.filter("tr").length, v = c.$tbodies.index(g.parents("tbody").filter(":first"));
                        for (c.parsers && c.parsers.length || f(b),
                        j = 0; t > j; j++) {
                            for (n = g[j].cells.length,
                            r = [],
                            q = {
                                child: [],
                                $row: g.eq(j),
                                order: c.cache[v].normalized.length
                            },
                            m = 0; n > m; m++)
                                o = "undefined" == typeof c.extractors[m].id ? u.getElementText(c, g[j].cells[m], m) : c.extractors[m].format(u.getElementText(c, g[j].cells[m], m), b, g[j].cells[m], m),
                                p = "no-parser" === c.parsers[m].id ? "" : c.parsers[m].format(o, b, g[j].cells[m], m),
                                r[m] = c.ignoreCase && "string" == typeof p ? p.toLowerCase() : p,
                                "numeric" === (c.parsers[m].type || "").toLowerCase() && (c.cache[v].colMax[m] = Math.max(Math.abs(r[m]) || 0, c.cache[v].colMax[m] || 0));
                            r.push(q),
                            c.cache[v].normalized.push(r)
                        }
                        s(c, h, i)
                    }
                }).bind("updateComplete" + c.namespace, function() {
                    b.isUpdating = !1
                }).bind("sorton" + c.namespace, function(c, f, i, j) {
                    var k = b.config;
                    c.stopPropagation(),
                    e.trigger("sortStart", this),
                    n(b, f),
                    m(b),
                    k.delayInit && d(k.cache) && g(b),
                    e.trigger("sortBegin", this),
                    q(b),
                    h(b, j),
                    e.trigger("sortEnd", this),
                    u.applyWidget(b),
                    a.isFunction(i) && i(b)
                }).bind("appendCache" + c.namespace, function(c, d, e) {
                    c.stopPropagation(),
                    h(b, e),
                    a.isFunction(d) && d(b)
                }).bind("updateCache" + c.namespace, function(d, e) {
                    c.parsers && c.parsers.length || f(b),
                    g(b),
                    a.isFunction(e) && e(b)
                }).bind("applyWidgetId" + c.namespace, function(a, d) {
                    a.stopPropagation(),
                    u.getWidgetById(d).format(b, c, c.widgetOptions)
                }).bind("applyWidgets" + c.namespace, function(a, c) {
                    a.stopPropagation(),
                    u.applyWidget(b, c)
                }).bind("refreshWidgets" + c.namespace, function(a, c, d) {
                    a.stopPropagation(),
                    u.refreshWidgets(b, c, d)
                }).bind("destroy" + c.namespace, function(a, c, d) {
                    a.stopPropagation(),
                    u.destroy(b, c, d)
                }).bind("resetToLoadState" + c.namespace, function() {
                    u.removeWidget(b, !0, !1),
                    c = a.extend(!0, u.defaults, c.originalSettings),
                    b.hasInitialized = !1,
                    u.setup(b, c)
                })
            }
            var u = this;
            u.version = "2.21.2",
            u.parsers = [],
            u.widgets = [],
            u.defaults = {
                theme: "default",
                widthFixed: !1,
                showProcessing: !1,
                headerTemplate: "{content}",
                onRenderTemplate: null ,
                onRenderHeader: null ,
                cancelSelection: !0,
                tabIndex: !0,
                dateFormat: "mmddyyyy",
                sortMultiSortKey: "shiftKey",
                sortResetKey: "ctrlKey",
                usNumberFormat: !0,
                delayInit: !1,
                serverSideSorting: !1,
                resort: !0,
                headers: {},
                ignoreCase: !0,
                sortForce: null ,
                sortList: [],
                sortAppend: null ,
                sortStable: !1,
                sortInitialOrder: "asc",
                sortLocaleCompare: !1,
                sortReset: !1,
                sortRestart: !1,
                emptyTo: "bottom",
                stringTo: "max",
                textExtraction: "basic",
                textAttribute: "data-text",
                textSorter: null ,
                numberSorter: null ,
                widgets: [],
                widgetOptions: {
                    zebra: ["even", "odd"]
                },
                initWidgets: !0,
                widgetClass: "widget-{name}",
                initialized: null ,
                tableClass: "",
                cssAsc: "",
                cssDesc: "",
                cssNone: "",
                cssHeader: "",
                cssHeaderRow: "",
                cssProcessing: "",
                cssChildRow: "tablesorter-childRow",
                cssIcon: "tablesorter-icon",
                cssIconNone: "",
                cssIconAsc: "",
                cssIconDesc: "",
                cssInfoBlock: "tablesorter-infoOnly",
                cssNoSort: "tablesorter-noSort",
                cssIgnoreRow: "tablesorter-ignoreRow",
                selectorHeaders: "> thead th, > thead td",
                selectorSort: "th, td",
                selectorRemove: ".remove-me",
                debug: !1,
                headerList: [],
                empties: {},
                strings: {},
                parsers: []
            },
            u.css = {
                table: "tablesorter",
                cssHasChild: "tablesorter-hasChildRow",
                childRow: "tablesorter-childRow",
                colgroup: "tablesorter-colgroup",
                header: "tablesorter-header",
                headerRow: "tablesorter-headerRow",
                headerIn: "tablesorter-header-inner",
                icon: "tablesorter-icon",
                processing: "tablesorter-processing",
                sortAsc: "tablesorter-headerAsc",
                sortDesc: "tablesorter-headerDesc",
                sortNone: "tablesorter-headerUnSorted"
            },
            u.language = {
                sortAsc: "Ascending sort applied, ",
                sortDesc: "Descending sort applied, ",
                sortNone: "No sort applied, ",
                nextAsc: "activate to apply an ascending sort",
                nextDesc: "activate to apply a descending sort",
                nextNone: "activate to remove the sort"
            },
            u.instanceMethods = {},
            u.log = b,
            u.benchmark = c,
            u.getElementText = function(b, c, d) {
                if (!c)
                    return "";
                var e, f = b.textExtraction || "", g = c.jquery ? c : a(c);
                return "string" == typeof f ? a.trim(("basic" === f ? g.attr(b.textAttribute) || c.textContent : c.textContent) || g.text() || "") : "function" == typeof f ? a.trim(f(g[0], b.table, d)) : "function" == typeof (e = u.getColumnData(b.table, f, d)) ? a.trim(e(g[0], b.table, d)) : a.trim(g[0].textContent || g.text() || "")
            }
            ,
            u.construct = function(b) {
                return this.each(function() {
                    var c = this
                      , d = a.extend(!0, {}, u.defaults, b, u.instanceMethods);
                    d.originalSettings = b,
                    !c.hasInitialized && u.buildTable && "TABLE" !== this.tagName ? u.buildTable(c, d) : u.setup(c, d)
                })
            }
            ,
            u.setup = function(c, d) {
                if (!c || !c.tHead || 0 === c.tBodies.length || c.hasInitialized === !0)
                    return d.debug ? b("ERROR: stopping initialization! No table, thead, tbody or tablesorter has already been initialized") : "";
                var e = ""
                  , h = a(c)
                  , i = a.metadata;
                c.hasInitialized = !1,
                c.isProcessing = !0,
                c.config = d,
                a.data(c, "tablesorter", d),
                d.debug && a.data(c, "startoveralltimer", new Date),
                d.supportsDataObject = function(a) {
                    return a[0] = parseInt(a[0], 10),
                    a[0] > 1 || 1 === a[0] && parseInt(a[1], 10) >= 4
                }(a.fn.jquery.split(".")),
                d.string = {
                    max: 1,
                    min: -1,
                    emptymin: 1,
                    emptymax: -1,
                    zero: 0,
                    none: 0,
                    "null": 0,
                    top: !0,
                    bottom: !1
                },
                d.emptyTo = d.emptyTo.toLowerCase(),
                d.stringTo = d.stringTo.toLowerCase(),
                /tablesorter\-/.test(h.attr("class")) || (e = "" !== d.theme ? " tablesorter-" + d.theme : ""),
                d.table = c,
                d.$table = h.addClass(u.css.table + " " + d.tableClass + e).attr("role", "grid"),
                d.$headers = h.find(d.selectorHeaders),
                d.namespace ? d.namespace = "." + d.namespace.replace(/\W/g, "") : d.namespace = ".tablesorter" + Math.random().toString(16).slice(2),
                d.$table.children().children("tr").attr("role", "row"),
                d.$tbodies = h.children("tbody:not(." + d.cssInfoBlock + ")").attr({
                    "aria-live": "polite",
                    "aria-relevant": "all"
                }),
                d.$table.children("caption").length && (e = d.$table.children("caption")[0],
                e.id || (e.id = d.namespace.slice(1) + "caption"),
                d.$table.attr("aria-labelledby", e.id)),
                d.widgetInit = {},
                d.textExtraction = d.$table.attr("data-text-extraction") || d.textExtraction || "basic",
                j(c),
                u.fixColumnWidth(c),
                u.applyWidgetOptions(c, d),
                f(c),
                d.totalRows = 0,
                d.delayInit || g(c),
                u.bindEvents(c, d.$headers, !0),
                t(c),
                d.supportsDataObject && "undefined" != typeof h.data().sortlist ? d.sortList = h.data().sortlist : i && h.metadata() && h.metadata().sortlist && (d.sortList = h.metadata().sortlist),
                u.applyWidget(c, !0),
                d.sortList.length > 0 ? h.trigger("sorton", [d.sortList, {}, !d.initWidgets, !0]) : (m(c),
                d.initWidgets && u.applyWidget(c, !1)),
                d.showProcessing && h.unbind("sortBegin" + d.namespace + " sortEnd" + d.namespace).bind("sortBegin" + d.namespace + " sortEnd" + d.namespace, function(a) {
                    clearTimeout(d.processTimer),
                    u.isProcessing(c),
                    "sortBegin" === a.type && (d.processTimer = setTimeout(function() {
                        u.isProcessing(c, !0)
                    }, 500))
                }),
                c.hasInitialized = !0,
                c.isProcessing = !1,
                d.debug && u.benchmark("Overall initialization time", a.data(c, "startoveralltimer")),
                h.trigger("tablesorter-initialized", c),
                "function" == typeof d.initialized && d.initialized(c)
            }
            ,
            u.fixColumnWidth = function(b) {
                b = a(b)[0];
                var c, d, e = b.config, f = e.$table.children("colgroup");
                f.length && f.hasClass(u.css.colgroup) && f.remove(),
                e.widthFixed && 0 === e.$table.children("colgroup").length && (f = a('<colgroup class="' + u.css.colgroup + '">'),
                c = e.$table.width(),
                e.$tbodies.find("tr:first").children(":visible").each(function() {
                    d = parseInt(a(this).width() / c * 1e3, 10) / 10 + "%",
                    f.append(a("<col>").css("width", d))
                }),
                e.$table.prepend(f))
            }
            ,
            u.getColumnData = function(b, c, d, e, f) {
                if ("undefined" != typeof c && null  !== c) {
                    b = a(b)[0];
                    var g, h, i = b.config, j = f || i.$headers, k = i.$headerIndexed && i.$headerIndexed[d] || j.filter('[data-column="' + d + '"]:last');
                    if (c[d])
                        return e ? c[d] : c[j.index(k)];
                    for (h in c)
                        if ("string" == typeof h && (g = k.filter(h).add(k.find(h)),
                        g.length))
                            return c[h]
                }
            }
            ,
            u.computeColumnIndex = function(b) {
                var c, d, e, f, g, h, i, j, k, l, m, n, o, p = [], q = {};
                for (c = 0; c < b.length; c++)
                    for (i = b[c].cells,
                    d = 0; d < i.length; d++) {
                        for (h = i[d],
                        g = a(h),
                        j = h.parentNode.rowIndex,
                        k = j + "-" + g.index(),
                        l = h.rowSpan || 1,
                        m = h.colSpan || 1,
                        "undefined" == typeof p[j] && (p[j] = []),
                        e = 0; e < p[j].length + 1; e++)
                            if ("undefined" == typeof p[j][e]) {
                                n = e;
                                break
                            }
                        for (q[k] = n,
                        g.attr({
                            "data-column": n
                        }),
                        e = j; j + l > e; e++)
                            for ("undefined" == typeof p[e] && (p[e] = []),
                            o = p[e],
                            f = n; n + m > f; f++)
                                o[f] = "x"
                    }
                return o.length
            }
            ,
            u.isProcessing = function(b, c, d) {
                b = a(b);
                var e = b[0].config
                  , f = d || b.find("." + u.css.header);
                c ? ("undefined" != typeof d && e.sortList.length > 0 && (f = f.filter(function() {
                    return this.sortDisabled ? !1 : u.isValueInArray(parseFloat(a(this).attr("data-column")), e.sortList) >= 0
                })),
                b.add(f).addClass(u.css.processing + " " + e.cssProcessing)) : b.add(f).removeClass(u.css.processing + " " + e.cssProcessing)
            }
            ,
            u.processTbody = function(b, c, d) {
                b = a(b)[0];
                var e;
                return d ? (b.isProcessing = !0,
                c.before('<span class="tablesorter-savemyplace"/>'),
                e = a.fn.detach ? c.detach() : c.remove()) : (e = a(b).find("span.tablesorter-savemyplace"),
                c.insertAfter(e),
                e.remove(),
                void (b.isProcessing = !1))
            }
            ,
            u.clearTableBody = function(b) {
                a(b)[0].config.$tbodies.children().detach()
            }
            ,
            u.bindEvents = function(b, c, e) {
                b = a(b)[0];
                var f, h = b.config;
                e !== !0 && (h.$extraHeaders = h.$extraHeaders ? h.$extraHeaders.add(c) : c),
                c.find(h.selectorSort).add(c.filter(h.selectorSort)).unbind("mousedown mouseup sort keyup ".split(" ").join(h.namespace + " ").replace(/\s+/g, " ")).bind("mousedown mouseup sort keyup ".split(" ").join(h.namespace + " "), function(e, i) {
                    var j, k = a(e.target), l = e.type;
                    if (!(1 !== (e.which || e.button) && !/sort|keyup/.test(l) || "keyup" === l && 13 !== e.which || "mouseup" === l && i !== !0 && (new Date).getTime() - f > 250)) {
                        if ("mousedown" === l)
                            return void (f = (new Date).getTime());
                        if (j = a.fn.closest ? k.closest("td,th") : k.parents("td,th").filter(":first"),
                        /(input|select|button|textarea)/i.test(e.target.tagName) || k.hasClass(h.cssNoSort) || k.parents("." + h.cssNoSort).length > 0 || k.parents("button").length > 0)
                            return !h.cancelSelection;
                        h.delayInit && d(h.cache) && g(b),
                        j = a.fn.closest ? a(this).closest("th, td")[0] : /TH|TD/.test(this.tagName) ? this : a(this).parents("th, td")[0],
                        j = h.$headers[c.index(j)],
                        j.sortDisabled || p(b, j, e)
                    }
                }),
                h.cancelSelection && c.attr("unselectable", "on").bind("selectstart", !1).css({
                    "user-select": "none",
                    MozUserSelect: "none"
                })
            }
            ,
            u.restoreHeaders = function(b) {
                var c, d = a(b)[0].config;
                d.$table.find(d.selectorHeaders).each(function(b) {
                    c = a(this),
                    c.find("." + u.css.headerIn).length && c.html(d.headerContent[b])
                })
            }
            ,
            u.destroy = function(b, c, d) {
                if (b = a(b)[0],
                b.hasInitialized) {
                    u.removeWidget(b, !0, !1);
                    var e, f = a(b), g = b.config, h = f.find("thead:first"), i = h.find("tr." + u.css.headerRow).removeClass(u.css.headerRow + " " + g.cssHeaderRow), j = f.find("tfoot:first > tr").children("th, td");
                    c === !1 && a.inArray("uitheme", g.widgets) >= 0 && (f.trigger("applyWidgetId", ["uitheme"]),
                    f.trigger("applyWidgetId", ["zebra"])),
                    h.find("tr").not(i).remove(),
                    e = "sortReset update updateAll updateRows updateCell addRows updateComplete sorton appendCache updateCache " + "applyWidgetId applyWidgets refreshWidgets destroy mouseup mouseleave keypress sortBegin sortEnd resetToLoadState ".split(" ").join(g.namespace + " "),
                    f.removeData("tablesorter").unbind(e.replace(/\s+/g, " ")),
                    g.$headers.add(j).removeClass([u.css.header, g.cssHeader, g.cssAsc, g.cssDesc, u.css.sortAsc, u.css.sortDesc, u.css.sortNone].join(" ")).removeAttr("data-column").removeAttr("aria-label").attr("aria-disabled", "true"),
                    i.find(g.selectorSort).unbind("mousedown mouseup keypress ".split(" ").join(g.namespace + " ").replace(/\s+/g, " ")),
                    u.restoreHeaders(b),
                    f.toggleClass(u.css.table + " " + g.tableClass + " tablesorter-" + g.theme, c === !1),
                    b.hasInitialized = !1,
                    delete b.config.cache,
                    "function" == typeof d && d(b)
                }
            }
            ,
            u.regex = {
                chunk: /(^([+\-]?(?:0|[1-9]\d*)(?:\.\d*)?(?:[eE][+\-]?\d+)?)?$|^0x[0-9a-f]+$|\d+)/gi,
                chunks: /(^\\0|\\0$)/,
                hex: /^0x[0-9a-f]+$/i
            },
            u.sortNatural = function(a, b) {
                if (a === b)
                    return 0;
                var c, d, e, f, g, h, i, j, k = u.regex;
                if (k.hex.test(b)) {
                    if (d = parseInt(a.match(k.hex), 16),
                    f = parseInt(b.match(k.hex), 16),
                    f > d)
                        return -1;
                    if (d > f)
                        return 1
                }
                for (c = a.replace(k.chunk, "\\0$1\\0").replace(k.chunks, "").split("\\0"),
                e = b.replace(k.chunk, "\\0$1\\0").replace(k.chunks, "").split("\\0"),
                j = Math.max(c.length, e.length),
                i = 0; j > i; i++) {
                    if (g = isNaN(c[i]) ? c[i] || 0 : parseFloat(c[i]) || 0,
                    h = isNaN(e[i]) ? e[i] || 0 : parseFloat(e[i]) || 0,
                    isNaN(g) !== isNaN(h))
                        return isNaN(g) ? 1 : -1;
                    if (typeof g != typeof h && (g += "",
                    h += ""),
                    h > g)
                        return -1;
                    if (g > h)
                        return 1
                }
                return 0
            }
            ,
            u.sortNaturalAsc = function(a, b, c, d, e) {
                if (a === b)
                    return 0;
                var f = e.string[e.empties[c] || e.emptyTo];
                return "" === a && 0 !== f ? "boolean" == typeof f ? f ? -1 : 1 : -f || -1 : "" === b && 0 !== f ? "boolean" == typeof f ? f ? 1 : -1 : f || 1 : u.sortNatural(a, b)
            }
            ,
            u.sortNaturalDesc = function(a, b, c, d, e) {
                if (a === b)
                    return 0;
                var f = e.string[e.empties[c] || e.emptyTo];
                return "" === a && 0 !== f ? "boolean" == typeof f ? f ? -1 : 1 : f || 1 : "" === b && 0 !== f ? "boolean" == typeof f ? f ? 1 : -1 : -f || -1 : u.sortNatural(b, a)
            }
            ,
            u.sortText = function(a, b) {
                return a > b ? 1 : b > a ? -1 : 0
            }
            ,
            u.getTextValue = function(a, b, c) {
                if (c) {
                    var d, e = a ? a.length : 0, f = c + b;
                    for (d = 0; e > d; d++)
                        f += a.charCodeAt(d);
                    return b * f
                }
                return 0
            }
            ,
            u.sortNumericAsc = function(a, b, c, d, e, f) {
                if (a === b)
                    return 0;
                var g = f.config
                  , h = g.string[g.empties[e] || g.emptyTo];
                return "" === a && 0 !== h ? "boolean" == typeof h ? h ? -1 : 1 : -h || -1 : "" === b && 0 !== h ? "boolean" == typeof h ? h ? 1 : -1 : h || 1 : (isNaN(a) && (a = u.getTextValue(a, c, d)),
                isNaN(b) && (b = u.getTextValue(b, c, d)),
                a - b)
            }
            ,
            u.sortNumericDesc = function(a, b, c, d, e, f) {
                if (a === b)
                    return 0;
                var g = f.config
                  , h = g.string[g.empties[e] || g.emptyTo];
                return "" === a && 0 !== h ? "boolean" == typeof h ? h ? -1 : 1 : h || 1 : "" === b && 0 !== h ? "boolean" == typeof h ? h ? 1 : -1 : -h || -1 : (isNaN(a) && (a = u.getTextValue(a, c, d)),
                isNaN(b) && (b = u.getTextValue(b, c, d)),
                b - a)
            }
            ,
            u.sortNumeric = function(a, b) {
                return a - b
            }
            ,
            u.characterEquivalents = {
                a: "áàâãäąå",
                A: "ÁÀÂÃÄĄÅ",
                c: "çćč",
                C: "ÇĆČ",
                e: "éèêëěę",
                E: "ÉÈÊËĚĘ",
                i: "íìİîïı",
                I: "ÍÌİÎÏ",
                o: "óòôõö",
                O: "ÓÒÔÕÖ",
                ss: "ß",
                SS: "ẞ",
                u: "úùûüů",
                U: "ÚÙÛÜŮ"
            },
            u.replaceAccents = function(a) {
                var b, c = "[", d = u.characterEquivalents;
                if (!u.characterRegex) {
                    u.characterRegexArray = {};
                    for (b in d)
                        "string" == typeof b && (c += d[b],
                        u.characterRegexArray[b] = new RegExp("[" + d[b] + "]","g"));
                    u.characterRegex = new RegExp(c + "]")
                }
                if (u.characterRegex.test(a))
                    for (b in d)
                        "string" == typeof b && (a = a.replace(u.characterRegexArray[b], b));
                return a
            }
            ,
            u.isValueInArray = function(a, b) {
                var c, d = b.length;
                for (c = 0; d > c; c++)
                    if (b[c][0] === a)
                        return c;
                return -1
            }
            ,
            u.addParser = function(a) {
                var b, c = u.parsers.length, d = !0;
                for (b = 0; c > b; b++)
                    u.parsers[b].id.toLowerCase() === a.id.toLowerCase() && (d = !1);
                d && u.parsers.push(a)
            }
            ,
            u.addInstanceMethods = function(b) {
                a.extend(u.instanceMethods, b)
            }
            ,
            u.getParserById = function(a) {
                if ("false" == a)
                    return !1;
                var b, c = u.parsers.length;
                for (b = 0; c > b; b++)
                    if (u.parsers[b].id.toLowerCase() === a.toString().toLowerCase())
                        return u.parsers[b];
                return !1
            }
            ,
            u.addWidget = function(a) {
                u.widgets.push(a)
            }
            ,
            u.hasWidget = function(b, c) {
                return b = a(b),
                b.length && b[0].config && b[0].config.widgetInit[c] || !1
            }
            ,
            u.getWidgetById = function(a) {
                var b, c, d = u.widgets.length;
                for (b = 0; d > b; b++)
                    if (c = u.widgets[b],
                    c && c.hasOwnProperty("id") && c.id.toLowerCase() === a.toLowerCase())
                        return c
            }
            ,
            u.applyWidgetOptions = function(b, c) {
                var d, e, f = c.widgets.length, g = c.widgetOptions;
                if (f)
                    for (d = 0; f > d; d++)
                        e = u.getWidgetById(c.widgets[d]),
                        e && "options" in e && (g = b.config.widgetOptions = a.extend(!0, {}, e.options, g))
            }
            ,
            u.applyWidget = function(b, d, e) {
                b = a(b)[0];
                var f, g, h, i, j, k, l, m = b.config, n = m.widgetOptions, o = " " + m.table.className + " ", p = [];
                if (d === !1 || !b.hasInitialized || !b.isApplyingWidgets && !b.isUpdating) {
                    if (m.debug && (i = new Date),
                    l = new RegExp("\\s" + m.widgetClass.replace(/\{name\}/i, "([\\w-]+)") + "\\s","g"),
                    o.match(l) && (k = o.match(l)))
                        for (g = k.length,
                        f = 0; g > f; f++)
                            m.widgets.push(k[f].replace(l, "$1"));
                    if (m.widgets.length) {
                        for (b.isApplyingWidgets = !0,
                        m.widgets = a.grep(m.widgets, function(b, c) {
                            return a.inArray(b, m.widgets) === c
                        }),
                        h = m.widgets || [],
                        g = h.length,
                        f = 0; g > f; f++)
                            l = u.getWidgetById(h[f]),
                            l && l.id && (l.priority || (l.priority = 10),
                            p[f] = l);
                        for (p.sort(function(a, b) {
                            return a.priority < b.priority ? -1 : a.priority === b.priority ? 0 : 1
                        }),
                        g = p.length,
                        f = 0; g > f; f++)
                            p[f] && (!d && m.widgetInit[p[f].id] || (m.widgetInit[p[f].id] = !0,
                            b.hasInitialized && u.applyWidgetOptions(b, m),
                            "init" in p[f] && (m.debug && (j = new Date),
                            p[f].init(b, p[f], m, n),
                            m.debug && u.benchmark("Initializing " + p[f].id + " widget", j))),
                            !d && "format" in p[f] && (m.debug && (j = new Date),
                            p[f].format(b, m, n, !1),
                            m.debug && u.benchmark((d ? "Initializing " : "Applying ") + p[f].id + " widget", j)));
                        d || "function" != typeof e || e(b)
                    }
                    setTimeout(function() {
                        b.isApplyingWidgets = !1,
                        a.data(b, "lastWidgetApplication", new Date)
                    }, 0),
                    m.debug && (k = m.widgets.length,
                    c("Completed " + (d === !0 ? "initializing " : "applying ") + k + " widget" + (1 !== k ? "s" : ""), i))
                }
            }
            ,
            u.removeWidget = function(c, d, e) {
                c = a(c)[0];
                var f, g, h, i, j = c.config;
                if (d === !0)
                    for (d = [],
                    i = u.widgets.length,
                    h = 0; i > h; h++)
                        g = u.widgets[h],
                        g && g.id && d.push(g.id);
                else
                    d = (a.isArray(d) ? d.join(",") : d || "").toLowerCase().split(/[\s,]+/);
                for (i = d.length,
                f = 0; i > f; f++)
                    g = u.getWidgetById(d[f]),
                    h = a.inArray(d[f], j.widgets),
                    g && "remove" in g && (j.debug && h >= 0 && b('Removing "' + d[f] + '" widget'),
                    g.remove(c, j, j.widgetOptions, e),
                    j.widgetInit[d[f]] = !1),
                    h >= 0 && e !== !0 && j.widgets.splice(h, 1)
            }
            ,
            u.refreshWidgets = function(b, c, d) {
                b = a(b)[0];
                var e, f = b.config, g = f.widgets, h = u.widgets, i = h.length, j = [], k = function(b) {
                    a(b).trigger("refreshComplete")
                }
                ;
                for (e = 0; i > e; e++)
                    h[e] && h[e].id && (c || a.inArray(h[e].id, g) < 0) && j.push(h[e].id);
                u.removeWidget(b, j.join(","), !0),
                d !== !0 ? (u.applyWidget(b, c || !1, k),
                c && u.applyWidget(b, !1, k)) : k(b)
            }
            ,
            u.getData = function(b, c, d) {
                var e, f, g = "", h = a(b);
                return h.length ? (e = a.metadata ? h.metadata() : !1,
                f = " " + (h.attr("class") || ""),
                "undefined" != typeof h.data(d) || "undefined" != typeof h.data(d.toLowerCase()) ? g += h.data(d) || h.data(d.toLowerCase()) : e && "undefined" != typeof e[d] ? g += e[d] : c && "undefined" != typeof c[d] ? g += c[d] : " " !== f && f.match(" " + d + "-") && (g = f.match(new RegExp("\\s" + d + "-([\\w-]+)"))[1] || ""),
                a.trim(g)) : ""
            }
            ,
            u.formatFloat = function(b, c) {
                if ("string" != typeof b || "" === b)
                    return b;
                var d, e = c && c.config ? c.config.usNumberFormat !== !1 : "undefined" != typeof c ? c : !0;
                return b = e ? b.replace(/,/g, "") : b.replace(/[\s|\.]/g, "").replace(/,/g, "."),
                /^\s*\([.\d]+\)/.test(b) && (b = b.replace(/^\s*\(([.\d]+)\)/, "-$1")),
                d = parseFloat(b),
                isNaN(d) ? a.trim(b) : d
            }
            ,
            u.isDigit = function(a) {
                return isNaN(a) ? /^[\-+(]?\d+[)]?$/.test(a.toString().replace(/[,.'"\s]/g, "")) : !0
            }
        }
    });
    var b = a.tablesorter;
    return a.fn.extend({
        tablesorter: b.construct
    }),
    b.addParser({
        id: "no-parser",
        is: function() {
            return !1
        },
        format: function() {
            return ""
        },
        type: "text"
    }),
    b.addParser({
        id: "text",
        is: function() {
            return !0
        },
        format: function(c, d) {
            var e = d.config;
            return c && (c = a.trim(e.ignoreCase ? c.toLocaleLowerCase() : c),
            c = e.sortLocaleCompare ? b.replaceAccents(c) : c),
            c
        },
        type: "text"
    }),
    b.addParser({
        id: "digit",
        is: function(a) {
            return b.isDigit(a)
        },
        format: function(c, d) {
            var e = b.formatFloat((c || "").replace(/[^\w,. \-()]/g, ""), d);
            return c && "number" == typeof e ? e : c ? a.trim(c && d.config.ignoreCase ? c.toLocaleLowerCase() : c) : c
        },
        type: "numeric"
    }),
    b.addParser({
        id: "currency",
        is: function(a) {
            return /^\(?\d+[\u00a3$\u20ac\u00a4\u00a5\u00a2?.]|[\u00a3$\u20ac\u00a4\u00a5\u00a2?.]\d+\)?$/.test((a || "").replace(/[+\-,. ]/g, ""))
        },
        format: function(c, d) {
            var e = b.formatFloat((c || "").replace(/[^\w,. \-()]/g, ""), d);
            return c && "number" == typeof e ? e : c ? a.trim(c && d.config.ignoreCase ? c.toLocaleLowerCase() : c) : c
        },
        type: "numeric"
    }),
    b.addParser({
        id: "url",
        is: function(a) {
            return /^(https?|ftp|file):\/\//.test(a)
        },
        format: function(b) {
            return b ? a.trim(b.replace(/(https?|ftp|file):\/\//, "")) : b
        },
        parsed: !0,
        type: "text"
    }),
    b.addParser({
        id: "isoDate",
        is: function(a) {
            return /^\d{4}[\/\-]\d{1,2}[\/\-]\d{1,2}/.test(a)
        },
        format: function(a, b) {
            var c = a ? new Date(a.replace(/-/g, "/")) : a;
            return c instanceof Date && isFinite(c) ? c.getTime() : a
        },
        type: "numeric"
    }),
    b.addParser({
        id: "percent",
        is: function(a) {
            return /(\d\s*?%|%\s*?\d)/.test(a) && a.length < 15
        },
        format: function(a, c) {
            return a ? b.formatFloat(a.replace(/%/g, ""), c) : a
        },
        type: "numeric"
    }),
    b.addParser({
        id: "image",
        is: function(a, b, c, d) {
            return d.find("img").length > 0
        },
        format: function(b, c, d) {
            return a(d).find("img").attr(c.config.imgAttr || "alt") || b
        },
        parsed: !0,
        type: "text"
    }),
    b.addParser({
        id: "usLongDate",
        is: function(a) {
            return /^[A-Z]{3,10}\.?\s+\d{1,2},?\s+(\d{4})(\s+\d{1,2}:\d{2}(:\d{2})?(\s+[AP]M)?)?$/i.test(a) || /^\d{1,2}\s+[A-Z]{3,10}\s+\d{4}/i.test(a)
        },
        format: function(a, b) {
            var c = a ? new Date(a.replace(/(\S)([AP]M)$/i, "$1 $2")) : a;
            return c instanceof Date && isFinite(c) ? c.getTime() : a
        },
        type: "numeric"
    }),
    b.addParser({
        id: "shortDate",
        is: function(a) {
            return /(^\d{1,2}[\/\s]\d{1,2}[\/\s]\d{4})|(^\d{4}[\/\s]\d{1,2}[\/\s]\d{1,2})/.test((a || "").replace(/\s+/g, " ").replace(/[\-.,]/g, "/"))
        },
        format: function(a, c, d, e) {
            if (a) {
                var f, g, h = c.config, i = h.$headerIndexed[e], j = i.length && i[0].dateFormat || b.getData(i, b.getColumnData(c, h.headers, e), "dateFormat") || h.dateFormat;
                return g = a.replace(/\s+/g, " ").replace(/[\-.,]/g, "/"),
                "mmddyyyy" === j ? g = g.replace(/(\d{1,2})[\/\s](\d{1,2})[\/\s](\d{4})/, "$3/$1/$2") : "ddmmyyyy" === j ? g = g.replace(/(\d{1,2})[\/\s](\d{1,2})[\/\s](\d{4})/, "$3/$2/$1") : "yyyymmdd" === j && (g = g.replace(/(\d{4})[\/\s](\d{1,2})[\/\s](\d{1,2})/, "$1/$2/$3")),
                f = new Date(g),
                f instanceof Date && isFinite(f) ? f.getTime() : a
            }
            return a
        },
        type: "numeric"
    }),
    b.addParser({
        id: "time",
        is: function(a) {
            return /^(([0-2]?\d:[0-5]\d)|([0-1]?\d:[0-5]\d\s?([AP]M)))$/i.test(a)
        },
        format: function(a, b) {
            var c = a ? new Date("2000/01/01 " + a.replace(/(\S)([AP]M)$/i, "$1 $2")) : a;
            return c instanceof Date && isFinite(c) ? c.getTime() : a
        },
        type: "numeric"
    }),
    b.addParser({
        id: "metadata",
        is: function() {
            return !1
        },
        format: function(b, c, d) {
            var e = c.config
              , f = e.parserMetadataName ? e.parserMetadataName : "sortValue";
            return a(d).metadata()[f]
        },
        type: "numeric"
    }),
    b.addWidget({
        id: "zebra",
        priority: 90,
        format: function(b, c, d) {
            var e, f, g, h, i, j, k, l = new RegExp(c.cssChildRow,"i"), m = c.$tbodies;
            for (c.debug && (j = new Date),
            k = 0; k < m.length; k++)
                h = 0,
                e = m.eq(k),
                f = e.children("tr:visible").not(c.selectorRemove),
                f.each(function() {
                    g = a(this),
                    l.test(this.className) || h++,
                    i = h % 2 === 0,
                    g.removeClass(d.zebra[i ? 1 : 0]).addClass(d.zebra[i ? 0 : 1])
                })
        },
        remove: function(a, c, d, e) {
            if (!e) {
                var f, g, h = c.$tbodies, i = (d.zebra || ["even", "odd"]).join(" ");
                for (f = 0; f < h.length; f++)
                    g = b.processTbody(a, h.eq(f), !0),
                    g.children().removeClass(i),
                    b.processTbody(a, g, !1)
            }
        }
    }),
    b
}),
!function(a) {
    a.fn.closestDescendant = function(b, c) {
        if (!b || "" === b)
            return a();
        c = !!c;
        var d = a();
        return this.each(function() {
            var e = a(this)
              , f = [];
            for (f.push(e); f.length > 0; )
                for (var g = f.shift(), h = g.children(), i = 0; i < h.length; ++i) {
                    var j = a(h[i]);
                    if (j.is(b)) {
                        if (d.push(j[0]),
                        !c)
                            return !1
                    } else
                        f.push(j)
                }
        }),
        d
    }
}(jQuery),
function(a) {
    a.deparam = function(b, c) {
        var d = {}
          , e = {
            "true": !0,
            "false": !1,
            "null": null 
        };
        return a.each(b.replace(/\+/g, " ").split("&"), function(b, f) {
            var g, h = f.split("="), i = decodeURIComponent(h[0]), j = d, k = 0, l = i.split("]["), m = l.length - 1;
            if (/\[/.test(l[0]) && /\]$/.test(l[m]) ? (l[m] = l[m].replace(/\]$/, ""),
            l = l.shift().split("[").concat(l),
            m = l.length - 1) : m = 0,
            2 === h.length)
                if (h = decodeURIComponent(h[1]),
                c && (h = h && !isNaN(h) ? +h : "undefined" === h ? void 0 : void 0 !== e[h] ? e[h] : h),
                m)
                    for (; m >= k; k++)
                        i = "" === l[k] ? j.length : l[k],
                        g = j[i] = m > k ? j[i] || (l[k + 1] && isNaN(l[k + 1]) ? {} : []) : h,
                        j = g;
                else
                    a.isArray(d[i]) ? d[i].push(h) : d[i] = void 0 !== d[i] ? [d[i], h] : h;
            else
                i && (d[i] = c ? void 0 : "")
        }),
        d
    }
}(jQuery),
!function(a) {
    var b;
    "undefined" != typeof window ? b = window : "undefined" != typeof self && (b = self),
    b.ALGOLIA_MIGRATION_LAYER = a()
}(function() {
    return function a(b, c, d) {
        function e(g, h) {
            if (!c[g]) {
                if (!b[g]) {
                    var i = "function" == typeof require && require;
                    if (!h && i)
                        return i(g, !0);
                    if (f)
                        return f(g, !0);
                    var j = new Error("Cannot find module '" + g + "'");
                    throw j.code = "MODULE_NOT_FOUND",
                    j
                }
                var k = c[g] = {
                    exports: {}
                };
                b[g][0].call(k.exports, function(a) {
                    var c = b[g][1][a];
                    return e(c ? c : a)
                }, k, k.exports, a, b, c, d)
            }
            return c[g].exports
        }
        for (var f = "function" == typeof require && require, g = 0; g < d.length; g++)
            e(d[g]);
        return e
    }({
        1: [function(a, b, c) {
            function d(a, b) {
                for (var c in b)
                    a.setAttribute(c, b[c])
            }
            function e(a, b) {
                a.onload = function() {
                    this.onerror = this.onload = null ,
                    b(null , a)
                }
                ,
                a.onerror = function() {
                    this.onerror = this.onload = null ,
                    b(new Error("Failed to load " + this.src), a)
                }
            }
            function f(a, b) {
                a.onreadystatechange = function() {
                    ("complete" == this.readyState || "loaded" == this.readyState) && (this.onreadystatechange = null ,
                    b(null , a))
                }
            }
            b.exports = function(a, b, c) {
                var g = document.head || document.getElementsByTagName("head")[0]
                  , h = document.createElement("script");
                "function" == typeof b && (c = b,
                b = {}),
                b = b || {},
                c = c || function() {}
                ,
                h.type = b.type || "text/javascript",
                h.charset = b.charset || "utf8",
                h.async = "async" in b ? !!b.async : !0,
                h.src = a,
                b.attrs && d(h, b.attrs),
                b.text && (h.text = "" + b.text);
                var i = "onload" in h ? e : f;
                i(h, c),
                h.onload || e(h, c),
                g.appendChild(h)
            }
        }
        , {}],
        2: [function(a, b, c) {
            "use strict";
            function d(a) {
                for (var b = new RegExp("cdn\\.jsdelivr\\.net/algoliasearch/latest/" + a.replace(".", "\\.") + "(?:\\.min)?\\.js$"), c = document.getElementsByTagName("script"), d = !1, e = 0, f = c.length; f > e; e++)
                    if (c[e].src && b.test(c[e].src)) {
                        d = !0;
                        break
                    }
                return d
            }
            b.exports = d
        }
        , {}],
        3: [function(a, b, c) {
            "use strict";
            function d(b) {
                var c = a(1)
                  , d = "//cdn.jsdelivr.net/algoliasearch/2/" + b + ".min.js"
                  , f = "-- AlgoliaSearch `latest` warning --\nWarning, you are using the `latest` version string from jsDelivr to load the AlgoliaSearch library.\nUsing `latest` is no more recommended, you should load //cdn.jsdelivr.net/algoliasearch/2/algoliasearch.min.js\n\nAlso, we updated the AlgoliaSearch JavaScript client to V3. If you want to upgrade,\nplease read our migration guide at https://github.com/algolia/algoliasearch-client-js/wiki/Migration-guide-from-2.x.x-to-3.x.x\n-- /AlgoliaSearch  `latest` warning --";
                window.console && (window.console.warn ? window.console.warn(f) : window.console.log && window.console.log(f));
                try {
                    document.write("<script>window.ALGOLIA_SUPPORTS_DOCWRITE = true</script>"),
                    window.ALGOLIA_SUPPORTS_DOCWRITE === !0 ? (document.write('<script src="' + d + '"></script>'),
                    e("document.write")()) : c(d, e("DOMElement"))
                } catch (g) {
                    c(d, e("DOMElement"))
                }
            }
            function e(a) {
                return function() {
                    var b = "AlgoliaSearch: loaded V2 script using " + a;
                    window.console && window.console.log && window.console.log(b)
                }
            }
            b.exports = d
        }
        , {
            1: 1
        }],
        4: [function(a, b, c) {
            "use strict";
            function d() {
                var a = "-- AlgoliaSearch V2 => V3 error --\nYou are trying to use a new version of the AlgoliaSearch JavaScript client with an old notation.\nPlease read our migration guide at https://github.com/algolia/algoliasearch-client-js/wiki/Migration-guide-from-2.x.x-to-3.x.x\n-- /AlgoliaSearch V2 => V3 error --";
                window.AlgoliaSearch = function() {
                    throw new Error(a)
                }
                ,
                window.AlgoliaSearchHelper = function() {
                    throw new Error(a)
                }
                ,
                window.AlgoliaExplainResults = function() {
                    throw new Error(a)
                }
            }
            b.exports = d
        }
        , {}],
        5: [function(a, b, c) {
            "use strict";
            function d(b) {
                var c = a(2)
                  , d = a(3)
                  , e = a(4);
                c(b) ? d(b) : e()
            }
            d("algoliasearch")
        }
        , {
            2: 2,
            3: 3,
            4: 4
        }]
    }, {}, [5])(5)
}),
function(a) {
    if ("object" == typeof exports && "undefined" != typeof module)
        module.exports = a();
    else if ("function" == typeof define && define.amd)
        define([], a);
    else {
        var b;
        b = "undefined" != typeof window ? window : "undefined" != typeof global ? global : "undefined" != typeof self ? self : this,
        b.algoliasearch = a()
    }
}(function() {
    var a;
    return function b(a, c, d) {
        function e(g, h) {
            if (!c[g]) {
                if (!a[g]) {
                    var i = "function" == typeof require && require;
                    if (!h && i)
                        return i(g, !0);
                    if (f)
                        return f(g, !0);
                    var j = new Error("Cannot find module '" + g + "'");
                    throw j.code = "MODULE_NOT_FOUND",
                    j
                }
                var k = c[g] = {
                    exports: {}
                };
                a[g][0].call(k.exports, function(b) {
                    var c = a[g][1][b];
                    return e(c ? c : b)
                }, k, k.exports, b, a, c, d)
            }
            return c[g].exports
        }
        for (var f = "function" == typeof require && require, g = 0; g < d.length; g++)
            e(d[g]);
        return e
    }({
        1: [function(a, b, c) {
            function d() {
                this._events = this._events || {},
                this._maxListeners = this._maxListeners || void 0
            }
            function e(a) {
                return "function" == typeof a
            }
            function f(a) {
                return "number" == typeof a
            }
            function g(a) {
                return "object" == typeof a && null  !== a
            }
            function h(a) {
                return void 0 === a
            }
            b.exports = d,
            d.EventEmitter = d,
            d.prototype._events = void 0,
            d.prototype._maxListeners = void 0,
            d.defaultMaxListeners = 10,
            d.prototype.setMaxListeners = function(a) {
                if (!f(a) || 0 > a || isNaN(a))
                    throw TypeError("n must be a positive number");
                return this._maxListeners = a,
                this
            }
            ,
            d.prototype.emit = function(a) {
                var b, c, d, f, i, j;
                if (this._events || (this._events = {}),
                "error" === a && (!this._events.error || g(this._events.error) && !this._events.error.length)) {
                    if (b = arguments[1],
                    b instanceof Error)
                        throw b;
                    throw TypeError('Uncaught, unspecified "error" event.')
                }
                if (c = this._events[a],
                h(c))
                    return !1;
                if (e(c))
                    switch (arguments.length) {
                    case 1:
                        c.call(this);
                        break;
                    case 2:
                        c.call(this, arguments[1]);
                        break;
                    case 3:
                        c.call(this, arguments[1], arguments[2]);
                        break;
                    default:
                        for (d = arguments.length,
                        f = new Array(d - 1),
                        i = 1; d > i; i++)
                            f[i - 1] = arguments[i];
                        c.apply(this, f)
                    }
                else if (g(c)) {
                    for (d = arguments.length,
                    f = new Array(d - 1),
                    i = 1; d > i; i++)
                        f[i - 1] = arguments[i];
                    for (j = c.slice(),
                    d = j.length,
                    i = 0; d > i; i++)
                        j[i].apply(this, f)
                }
                return !0
            }
            ,
            d.prototype.addListener = function(a, b) {
                var c;
                if (!e(b))
                    throw TypeError("listener must be a function");
                if (this._events || (this._events = {}),
                this._events.newListener && this.emit("newListener", a, e(b.listener) ? b.listener : b),
                this._events[a] ? g(this._events[a]) ? this._events[a].push(b) : this._events[a] = [this._events[a], b] : this._events[a] = b,
                g(this._events[a]) && !this._events[a].warned) {
                    var c;
                    c = h(this._maxListeners) ? d.defaultMaxListeners : this._maxListeners,
                    c && c > 0 && this._events[a].length > c && (this._events[a].warned = !0,
                    console.error("(node) warning: possible EventEmitter memory leak detected. %d listeners added. Use emitter.setMaxListeners() to increase limit.", this._events[a].length),
                    "function" == typeof console.trace && console.trace())
                }
                return this
            }
            ,
            d.prototype.on = d.prototype.addListener,
            d.prototype.once = function(a, b) {
                function c() {
                    this.removeListener(a, c),
                    d || (d = !0,
                    b.apply(this, arguments))
                }
                if (!e(b))
                    throw TypeError("listener must be a function");
                var d = !1;
                return c.listener = b,
                this.on(a, c),
                this
            }
            ,
            d.prototype.removeListener = function(a, b) {
                var c, d, f, h;
                if (!e(b))
                    throw TypeError("listener must be a function");
                if (!this._events || !this._events[a])
                    return this;
                if (c = this._events[a],
                f = c.length,
                d = -1,
                c === b || e(c.listener) && c.listener === b)
                    delete this._events[a],
                    this._events.removeListener && this.emit("removeListener", a, b);
                else if (g(c)) {
                    for (h = f; h-- > 0; )
                        if (c[h] === b || c[h].listener && c[h].listener === b) {
                            d = h;
                            break
                        }
                    if (0 > d)
                        return this;
                    1 === c.length ? (c.length = 0,
                    delete this._events[a]) : c.splice(d, 1),
                    this._events.removeListener && this.emit("removeListener", a, b)
                }
                return this
            }
            ,
            d.prototype.removeAllListeners = function(a) {
                var b, c;
                if (!this._events)
                    return this;
                if (!this._events.removeListener)
                    return 0 === arguments.length ? this._events = {} : this._events[a] && delete this._events[a],
                    this;
                if (0 === arguments.length) {
                    for (b in this._events)
                        "removeListener" !== b && this.removeAllListeners(b);
                    return this.removeAllListeners("removeListener"),
                    this._events = {},
                    this
                }
                if (c = this._events[a],
                e(c))
                    this.removeListener(a, c);
                else
                    for (; c.length; )
                        this.removeListener(a, c[c.length - 1]);
                return delete this._events[a],
                this
            }
            ,
            d.prototype.listeners = function(a) {
                var b;
                return b = this._events && this._events[a] ? e(this._events[a]) ? [this._events[a]] : this._events[a].slice() : []
            }
            ,
            d.listenerCount = function(a, b) {
                var c;
                return c = a._events && a._events[b] ? e(a._events[b]) ? 1 : a._events[b].length : 0
            }
        }
        , {}],
        2: [function(a, b, c) {
            function d() {
                k = !1,
                h.length ? j = h.concat(j) : l = -1,
                j.length && e()
            }
            function e() {
                if (!k) {
                    var a = setTimeout(d);
                    k = !0;
                    for (var b = j.length; b; ) {
                        for (h = j,
                        j = []; ++l < b; )
                            h && h[l].run();
                        l = -1,
                        b = j.length
                    }
                    h = null ,
                    k = !1,
                    clearTimeout(a)
                }
            }
            function f(a, b) {
                this.fun = a,
                this.array = b
            }
            function g() {}
            var h, i = b.exports = {}, j = [], k = !1, l = -1;
            i.nextTick = function(a) {
                var b = new Array(arguments.length - 1);
                if (arguments.length > 1)
                    for (var c = 1; c < arguments.length; c++)
                        b[c - 1] = arguments[c];
                j.push(new f(a,b)),
                1 !== j.length || k || setTimeout(e, 0)
            }
            ,
            f.prototype.run = function() {
                this.fun.apply(null , this.array)
            }
            ,
            i.title = "browser",
            i.browser = !0,
            i.env = {},
            i.argv = [],
            i.version = "",
            i.versions = {},
            i.on = g,
            i.addListener = g,
            i.once = g,
            i.off = g,
            i.removeListener = g,
            i.removeAllListeners = g,
            i.emit = g,
            i.binding = function(a) {
                throw new Error("process.binding is not supported")
            }
            ,
            i.cwd = function() {
                return "/"
            }
            ,
            i.chdir = function(a) {
                throw new Error("process.chdir is not supported")
            }
            ,
            i.umask = function() {
                return 0
            }
        }
        , {}],
        3: [function(a, b, c) {
            "use strict";
            function d(a, b) {
                return Object.prototype.hasOwnProperty.call(a, b)
            }
            b.exports = function(a, b, c, f) {
                b = b || "&",
                c = c || "=";
                var g = {};
                if ("string" != typeof a || 0 === a.length)
                    return g;
                var h = /\+/g;
                a = a.split(b);
                var i = 1e3;
                f && "number" == typeof f.maxKeys && (i = f.maxKeys);
                var j = a.length;
                i > 0 && j > i && (j = i);
                for (var k = 0; j > k; ++k) {
                    var l, m, n, o, p = a[k].replace(h, "%20"), q = p.indexOf(c);
                    q >= 0 ? (l = p.substr(0, q),
                    m = p.substr(q + 1)) : (l = p,
                    m = ""),
                    n = decodeURIComponent(l),
                    o = decodeURIComponent(m),
                    d(g, n) ? e(g[n]) ? g[n].push(o) : g[n] = [g[n], o] : g[n] = o
                }
                return g
            }
            ;
            var e = Array.isArray || function(a) {
                return "[object Array]" === Object.prototype.toString.call(a)
            }
        }
        , {}],
        4: [function(a, b, c) {
            "use strict";
            function d(a, b) {
                if (a.map)
                    return a.map(b);
                for (var c = [], d = 0; d < a.length; d++)
                    c.push(b(a[d], d));
                return c
            }
            var e = function(a) {
                switch (typeof a) {
                case "string":
                    return a;
                case "boolean":
                    return a ? "true" : "false";
                case "number":
                    return isFinite(a) ? a : "";
                default:
                    return ""
                }
            }
            ;
            b.exports = function(a, b, c, h) {
                return b = b || "&",
                c = c || "=",
                null  === a && (a = void 0),
                "object" == typeof a ? d(g(a), function(g) {
                    var h = encodeURIComponent(e(g)) + c;
                    return f(a[g]) ? d(a[g], function(a) {
                        return h + encodeURIComponent(e(a))
                    }).join(b) : h + encodeURIComponent(e(a[g]))
                }).join(b) : h ? encodeURIComponent(e(h)) + c + encodeURIComponent(e(a)) : ""
            }
            ;
            var f = Array.isArray || function(a) {
                return "[object Array]" === Object.prototype.toString.call(a)
            }
              , g = Object.keys || function(a) {
                var b = [];
                for (var c in a)
                    Object.prototype.hasOwnProperty.call(a, c) && b.push(c);
                return b
            }
        }
        , {}],
        5: [function(a, b, c) {
            "use strict";
            c.decode = c.parse = a(3),
            c.encode = c.stringify = a(4)
        }
        , {
            3: 3,
            4: 4
        }],
        6: [function(a, b, c) {
            function d() {
                return "WebkitAppearance" in document.documentElement.style || window.console && (console.firebug || console.exception && console.table) || navigator.userAgent.toLowerCase().match(/firefox\/(\d+)/) && parseInt(RegExp.$1, 10) >= 31
            }
            function e() {
                var a = arguments
                  , b = this.useColors;
                if (a[0] = (b ? "%c" : "") + this.namespace + (b ? " %c" : " ") + a[0] + (b ? "%c " : " ") + "+" + c.humanize(this.diff),
                !b)
                    return a;
                var d = "color: " + this.color;
                a = [a[0], d, "color: inherit"].concat(Array.prototype.slice.call(a, 1));
                var e = 0
                  , f = 0;
                return a[0].replace(/%[a-z%]/g, function(a) {
                    "%%" !== a && (e++,
                    "%c" === a && (f = e))
                }),
                a.splice(f, 0, d),
                a
            }
            function f() {
                return "object" == typeof console && console.log && Function.prototype.apply.call(console.log, console, arguments)
            }
            function g(a) {
                try {
                    null  == a ? c.storage.removeItem("debug") : c.storage.debug = a
                } catch (b) {}
            }
            function h() {
                var a;
                try {
                    a = c.storage.debug
                } catch (b) {}
                return a
            }
            function i() {
                try {
                    return window.localStorage
                } catch (a) {}
            }
            c = b.exports = a(7),
            c.log = f,
            c.formatArgs = e,
            c.save = g,
            c.load = h,
            c.useColors = d,
            c.storage = "undefined" != typeof chrome && "undefined" != typeof chrome.storage ? chrome.storage.local : i(),
            c.colors = ["lightseagreen", "forestgreen", "goldenrod", "dodgerblue", "darkorchid", "crimson"],
            c.formatters.j = function(a) {
                return JSON.stringify(a)
            }
            ,
            c.enable(h())
        }
        , {
            7: 7
        }],
        7: [function(a, b, c) {
            function d() {
                return c.colors[k++ % c.colors.length]
            }
            function e(a) {
                function b() {}
                function e() {
                    var a = e
                      , b = +new Date
                      , f = b - (j || b);
                    a.diff = f,
                    a.prev = j,
                    a.curr = b,
                    j = b,
                    null  == a.useColors && (a.useColors = c.useColors()),
                    null  == a.color && a.useColors && (a.color = d());
                    var g = Array.prototype.slice.call(arguments);
                    g[0] = c.coerce(g[0]),
                    "string" != typeof g[0] && (g = ["%o"].concat(g));
                    var h = 0;
                    g[0] = g[0].replace(/%([a-z%])/g, function(b, d) {
                        if ("%%" === b)
                            return b;
                        h++;
                        var e = c.formatters[d];
                        if ("function" == typeof e) {
                            var f = g[h];
                            b = e.call(a, f),
                            g.splice(h, 1),
                            h--
                        }
                        return b
                    }),
                    "function" == typeof c.formatArgs && (g = c.formatArgs.apply(a, g));
                    var i = e.log || c.log || console.log.bind(console);
                    i.apply(a, g)
                }
                b.enabled = !1,
                e.enabled = !0;
                var f = c.enabled(a) ? e : b;
                return f.namespace = a,
                f
            }
            function f(a) {
                c.save(a);
                for (var b = (a || "").split(/[\s,]+/), d = b.length, e = 0; d > e; e++)
                    b[e] && (a = b[e].replace(/\*/g, ".*?"),
                    "-" === a[0] ? c.skips.push(new RegExp("^" + a.substr(1) + "$")) : c.names.push(new RegExp("^" + a + "$")))
            }
            function g() {
                c.enable("")
            }
            function h(a) {
                var b, d;
                for (b = 0,
                d = c.skips.length; d > b; b++)
                    if (c.skips[b].test(a))
                        return !1;
                for (b = 0,
                d = c.names.length; d > b; b++)
                    if (c.names[b].test(a))
                        return !0;
                return !1
            }
            function i(a) {
                return a instanceof Error ? a.stack || a.message : a
            }
            c = b.exports = e,
            c.coerce = i,
            c.disable = g,
            c.enable = f,
            c.enabled = h,
            c.humanize = a(8),
            c.names = [],
            c.skips = [],
            c.formatters = {};
            var j, k = 0
        }
        , {
            8: 8
        }],
        8: [function(a, b, c) {
            function d(a) {
                if (a = "" + a,
                !(a.length > 1e4)) {
                    var b = /^((?:\d+)?\.?\d+) *(milliseconds?|msecs?|ms|seconds?|secs?|s|minutes?|mins?|m|hours?|hrs?|h|days?|d|years?|yrs?|y)?$/i.exec(a);
                    if (b) {
                        var c = parseFloat(b[1])
                          , d = (b[2] || "ms").toLowerCase();
                        switch (d) {
                        case "years":
                        case "year":
                        case "yrs":
                        case "yr":
                        case "y":
                            return c * l;
                        case "days":
                        case "day":
                        case "d":
                            return c * k;
                        case "hours":
                        case "hour":
                        case "hrs":
                        case "hr":
                        case "h":
                            return c * j;
                        case "minutes":
                        case "minute":
                        case "mins":
                        case "min":
                        case "m":
                            return c * i;
                        case "seconds":
                        case "second":
                        case "secs":
                        case "sec":
                        case "s":
                            return c * h;
                        case "milliseconds":
                        case "millisecond":
                        case "msecs":
                        case "msec":
                        case "ms":
                            return c
                        }
                    }
                }
            }
            function e(a) {
                return a >= k ? Math.round(a / k) + "d" : a >= j ? Math.round(a / j) + "h" : a >= i ? Math.round(a / i) + "m" : a >= h ? Math.round(a / h) + "s" : a + "ms"
            }
            function f(a) {
                return g(a, k, "day") || g(a, j, "hour") || g(a, i, "minute") || g(a, h, "second") || a + " ms"
            }
            function g(a, b, c) {
                return b > a ? void 0 : 1.5 * b > a ? Math.floor(a / b) + " " + c : Math.ceil(a / b) + " " + c + "s"
            }
            var h = 1e3
              , i = 60 * h
              , j = 60 * i
              , k = 24 * j
              , l = 365.25 * k;
            b.exports = function(a, b) {
                return b = b || {},
                "string" == typeof a ? d(a) : b["long"] ? f(a) : e(a)
            }
        }
        , {}],
        9: [function(b, c, d) {
            (function(d, e) {
                (function() {
                    "use strict";
                    function f(a) {
                        return "function" == typeof a || "object" == typeof a && null  !== a
                    }
                    function g(a) {
                        return "function" == typeof a
                    }
                    function h(a) {
                        return "object" == typeof a && null  !== a
                    }
                    function i(a) {
                        U = a
                    }
                    function j(a) {
                        Y = a
                    }
                    function k() {
                        return function() {
                            d.nextTick(p)
                        }
                    }
                    function l() {
                        return function() {
                            T(p)
                        }
                    }
                    function m() {
                        var a = 0
                          , b = new _(p)
                          , c = document.createTextNode("");
                        return b.observe(c, {
                            characterData: !0
                        }),
                        function() {
                            c.data = a = ++a % 2
                        }
                    }
                    function n() {
                        var a = new MessageChannel;
                        return a.port1.onmessage = p,
                        function() {
                            a.port2.postMessage(0)
                        }
                    }
                    function o() {
                        return function() {
                            setTimeout(p, 1)
                        }
                    }
                    function p() {
                        for (var a = 0; X > a; a += 2) {
                            var b = ca[a]
                              , c = ca[a + 1];
                            b(c),
                            ca[a] = void 0,
                            ca[a + 1] = void 0
                        }
                        X = 0
                    }
                    function q() {
                        try {
                            var a = b
                              , c = a("vertx");
                            return T = c.runOnLoop || c.runOnContext,
                            l()
                        } catch (d) {
                            return o()
                        }
                    }
                    function r() {}
                    function s() {
                        return new TypeError("You cannot resolve a promise with itself")
                    }
                    function t() {
                        return new TypeError("A promises callback cannot return that same promise.")
                    }
                    function u(a) {
                        try {
                            return a.then
                        } catch (b) {
                            return ga.error = b,
                            ga
                        }
                    }
                    function v(a, b, c, d) {
                        try {
                            a.call(b, c, d)
                        } catch (e) {
                            return e
                        }
                    }
                    function w(a, b, c) {
                        Y(function(a) {
                            var d = !1
                              , e = v(c, b, function(c) {
                                d || (d = !0,
                                b !== c ? z(a, c) : B(a, c))
                            }, function(b) {
                                d || (d = !0,
                                C(a, b))
                            }, "Settle: " + (a._label || " unknown promise"));
                            !d && e && (d = !0,
                            C(a, e))
                        }, a)
                    }
                    function x(a, b) {
                        b._state === ea ? B(a, b._result) : b._state === fa ? C(a, b._result) : D(b, void 0, function(b) {
                            z(a, b)
                        }, function(b) {
                            C(a, b)
                        })
                    }
                    function y(a, b) {
                        if (b.constructor === a.constructor)
                            x(a, b);
                        else {
                            var c = u(b);
                            c === ga ? C(a, ga.error) : void 0 === c ? B(a, b) : g(c) ? w(a, b, c) : B(a, b)
                        }
                    }
                    function z(a, b) {
                        a === b ? C(a, s()) : f(b) ? y(a, b) : B(a, b)
                    }
                    function A(a) {
                        a._onerror && a._onerror(a._result),
                        E(a)
                    }
                    function B(a, b) {
                        a._state === da && (a._result = b,
                        a._state = ea,
                        0 !== a._subscribers.length && Y(E, a))
                    }
                    function C(a, b) {
                        a._state === da && (a._state = fa,
                        a._result = b,
                        Y(A, a))
                    }
                    function D(a, b, c, d) {
                        var e = a._subscribers
                          , f = e.length;
                        a._onerror = null ,
                        e[f] = b,
                        e[f + ea] = c,
                        e[f + fa] = d,
                        0 === f && a._state && Y(E, a)
                    }
                    function E(a) {
                        var b = a._subscribers
                          , c = a._state;
                        if (0 !== b.length) {
                            for (var d, e, f = a._result, g = 0; g < b.length; g += 3)
                                d = b[g],
                                e = b[g + c],
                                d ? H(c, d, e, f) : e(f);
                            a._subscribers.length = 0
                        }
                    }
                    function F() {
                        this.error = null 
                    }
                    function G(a, b) {
                        try {
                            return a(b)
                        } catch (c) {
                            return ha.error = c,
                            ha
                        }
                    }
                    function H(a, b, c, d) {
                        var e, f, h, i, j = g(c);
                        if (j) {
                            if (e = G(c, d),
                            e === ha ? (i = !0,
                            f = e.error,
                            e = null ) : h = !0,
                            b === e)
                                return void C(b, t())
                        } else
                            e = d,
                            h = !0;
                        b._state !== da || (j && h ? z(b, e) : i ? C(b, f) : a === ea ? B(b, e) : a === fa && C(b, e))
                    }
                    function I(a, b) {
                        try {
                            b(function(b) {
                                z(a, b)
                            }, function(b) {
                                C(a, b)
                            })
                        } catch (c) {
                            C(a, c)
                        }
                    }
                    function J(a, b) {
                        var c = this;
                        c._instanceConstructor = a,
                        c.promise = new a(r),
                        c._validateInput(b) ? (c._input = b,
                        c.length = b.length,
                        c._remaining = b.length,
                        c._init(),
                        0 === c.length ? B(c.promise, c._result) : (c.length = c.length || 0,
                        c._enumerate(),
                        0 === c._remaining && B(c.promise, c._result))) : C(c.promise, c._validationError())
                    }
                    function K(a) {
                        return new ia(this,a).promise
                    }
                    function L(a) {
                        function b(a) {
                            z(e, a)
                        }
                        function c(a) {
                            C(e, a)
                        }
                        var d = this
                          , e = new d(r);
                        if (!W(a))
                            return C(e, new TypeError("You must pass an array to race.")),
                            e;
                        for (var f = a.length, g = 0; e._state === da && f > g; g++)
                            D(d.resolve(a[g]), void 0, b, c);
                        return e
                    }
                    function M(a) {
                        var b = this;
                        if (a && "object" == typeof a && a.constructor === b)
                            return a;
                        var c = new b(r);
                        return z(c, a),
                        c
                    }
                    function N(a) {
                        var b = this
                          , c = new b(r);
                        return C(c, a),
                        c
                    }
                    function O() {
                        throw new TypeError("You must pass a resolver function as the first argument to the promise constructor")
                    }
                    function P() {
                        throw new TypeError("Failed to construct 'Promise': Please use the 'new' operator, this object constructor cannot be called as a function.")
                    }
                    function Q(a) {
                        this._id = na++,
                        this._state = void 0,
                        this._result = void 0,
                        this._subscribers = [],
                        r !== a && (g(a) || O(),
                        this instanceof Q || P(),
                        I(this, a))
                    }
                    function R() {
                        var a;
                        if ("undefined" != typeof e)
                            a = e;
                        else if ("undefined" != typeof self)
                            a = self;
                        else
                            try {
                                a = Function("return this")()
                            } catch (b) {
                                throw new Error("polyfill failed because global object is unavailable in this environment")
                            }
                        var c = a.Promise;
                        (!c || "[object Promise]" !== Object.prototype.toString.call(c.resolve()) || c.cast) && (a.Promise = oa)
                    }
                    var S;
                    S = Array.isArray ? Array.isArray : function(a) {
                        return "[object Array]" === Object.prototype.toString.call(a)
                    }
                    ;
                    var T, U, V, W = S, X = 0, Y = ({}.toString,
                    function(a, b) {
                        ca[X] = a,
                        ca[X + 1] = b,
                        X += 2,
                        2 === X && (U ? U(p) : V())
                    }
                    ), Z = "undefined" != typeof window ? window : void 0, $ = Z || {}, _ = $.MutationObserver || $.WebKitMutationObserver, aa = "undefined" != typeof d && "[object process]" === {}.toString.call(d), ba = "undefined" != typeof Uint8ClampedArray && "undefined" != typeof importScripts && "undefined" != typeof MessageChannel, ca = new Array(1e3);
                    V = aa ? k() : _ ? m() : ba ? n() : void 0 === Z && "function" == typeof b ? q() : o();
                    var da = void 0
                      , ea = 1
                      , fa = 2
                      , ga = new F
                      , ha = new F;
                    J.prototype._validateInput = function(a) {
                        return W(a)
                    }
                    ,
                    J.prototype._validationError = function() {
                        return new Error("Array Methods must be provided an Array")
                    }
                    ,
                    J.prototype._init = function() {
                        this._result = new Array(this.length)
                    }
                    ;
                    var ia = J;
                    J.prototype._enumerate = function() {
                        for (var a = this, b = a.length, c = a.promise, d = a._input, e = 0; c._state === da && b > e; e++)
                            a._eachEntry(d[e], e)
                    }
                    ,
                    J.prototype._eachEntry = function(a, b) {
                        var c = this
                          , d = c._instanceConstructor;
                        h(a) ? a.constructor === d && a._state !== da ? (a._onerror = null ,
                        c._settledAt(a._state, b, a._result)) : c._willSettleAt(d.resolve(a), b) : (c._remaining--,
                        c._result[b] = a)
                    }
                    ,
                    J.prototype._settledAt = function(a, b, c) {
                        var d = this
                          , e = d.promise;
                        e._state === da && (d._remaining--,
                        a === fa ? C(e, c) : d._result[b] = c),
                        0 === d._remaining && B(e, d._result)
                    }
                    ,
                    J.prototype._willSettleAt = function(a, b) {
                        var c = this;
                        D(a, void 0, function(a) {
                            c._settledAt(ea, b, a)
                        }, function(a) {
                            c._settledAt(fa, b, a)
                        })
                    }
                    ;
                    var ja = K
                      , ka = L
                      , la = M
                      , ma = N
                      , na = 0
                      , oa = Q;
                    Q.all = ja,
                    Q.race = ka,
                    Q.resolve = la,
                    Q.reject = ma,
                    Q._setScheduler = i,
                    Q._setAsap = j,
                    Q._asap = Y,
                    Q.prototype = {
                        constructor: Q,
                        then: function(a, b) {
                            var c = this
                              , d = c._state;
                            if (d === ea && !a || d === fa && !b)
                                return this;
                            var e = new this.constructor(r)
                              , f = c._result;
                            if (d) {
                                var g = arguments[d - 1];
                                Y(function() {
                                    H(d, e, g, f)
                                })
                            } else
                                D(c, e, a, b);
                            return e
                        },
                        "catch": function(a) {
                            return this.then(null , a)
                        }
                    };
                    var pa = R
                      , qa = {
                        Promise: oa,
                        polyfill: pa
                    };
                    "function" == typeof a && a.amd ? a(function() {
                        return qa
                    }) : "undefined" != typeof c && c.exports ? c.exports = qa : "undefined" != typeof this && (this.ES6Promise = qa),
                    pa()
                }
                ).call(this)
            }
            ).call(this, b(2), "undefined" != typeof global ? global : "undefined" != typeof self ? self : "undefined" != typeof window ? window : {})
        }
        , {
            2: 2
        }],
        10: [function(a, b, c) {
            "function" == typeof Object.create ? b.exports = function(a, b) {
                a.super_ = b,
                a.prototype = Object.create(b.prototype, {
                    constructor: {
                        value: a,
                        enumerable: !1,
                        writable: !0,
                        configurable: !0
                    }
                })
            }
             : b.exports = function(a, b) {
                a.super_ = b;
                var c = function() {}
                ;
                c.prototype = b.prototype,
                a.prototype = new c,
                a.prototype.constructor = a
            }
        }
        , {}],
        11: [function(a, b, c) {
            function d(a) {
                var b = a ? a.length : 0;
                return b ? a[b - 1] : void 0
            }
            b.exports = d
        }
        , {}],
        12: [function(a, b, c) {
            var d = a(16)
              , e = a(23)
              , f = a(45)
              , g = f(d, e);
            b.exports = g
        }
        , {
            16: 16,
            23: 23,
            45: 45
        }],
        13: [function(a, b, c) {
            function d(a, b, c) {
                var d = h(a) ? e : g;
                return b = f(b, c, 3),
                d(a, b)
            }
            var e = a(17)
              , f = a(20)
              , g = a(31)
              , h = a(68);
            b.exports = d
        }
        , {
            17: 17,
            20: 20,
            31: 31,
            68: 68
        }],
        14: [function(a, b, c) {
            function d(a, b) {
                if ("function" != typeof a)
                    throw new TypeError(e);
                return b = f(void 0 === b ? a.length - 1 : +b || 0, 0),
                function() {
                    for (var c = arguments, d = -1, e = f(c.length - b, 0), g = Array(e); ++d < e; )
                        g[d] = c[b + d];
                    switch (b) {
                    case 0:
                        return a.call(this, g);
                    case 1:
                        return a.call(this, c[0], g);
                    case 2:
                        return a.call(this, c[0], c[1], g)
                    }
                    var h = Array(b + 1);
                    for (d = -1; ++d < b; )
                        h[d] = c[d];
                    return h[b] = g,
                    a.apply(this, h)
                }
            }
            var e = "Expected a function"
              , f = Math.max;
            b.exports = d
        }
        , {}],
        15: [function(a, b, c) {
            function d(a, b) {
                var c = -1
                  , d = a.length;
                for (b || (b = Array(d)); ++c < d; )
                    b[c] = a[c];
                return b
            }
            b.exports = d
        }
        , {}],
        16: [function(a, b, c) {
            function d(a, b) {
                for (var c = -1, d = a.length; ++c < d && b(a[c], c, a) !== !1; )
                    ;
                return a
            }
            b.exports = d
        }
        , {}],
        17: [function(a, b, c) {
            function d(a, b) {
                for (var c = -1, d = a.length, e = Array(d); ++c < d; )
                    e[c] = b(a[c], c, a);
                return e
            }
            b.exports = d
        }
        , {}],
        18: [function(a, b, c) {
            function d(a, b) {
                for (var c = -1, d = a.length; ++c < d; )
                    if (b(a[c], c, a))
                        return !0;
                return !1
            }
            b.exports = d
        }
        , {}],
        19: [function(a, b, c) {
            function d(a, b) {
                return null  == b ? a : e(b, f(b), a)
            }
            var e = a(22)
              , f = a(75);
            b.exports = d
        }
        , {
            22: 22,
            75: 75
        }],
        20: [function(a, b, c) {
            function d(a, b, c) {
                var d = typeof a;
                return "function" == d ? void 0 === b ? a : g(a, b, c) : null  == a ? h : "object" == d ? e(a) : void 0 === b ? i(a) : f(a, b)
            }
            var e = a(32)
              , f = a(33)
              , g = a(40)
              , h = a(79)
              , i = a(80);
            b.exports = d
        }
        , {
            32: 32,
            33: 33,
            40: 40,
            79: 79,
            80: 80
        }],
        21: [function(a, b, c) {
            function d(a, b, c, o, p, q, r) {
                var t;
                if (c && (t = p ? c(a, o, p) : c(a)),
                void 0 !== t)
                    return t;
                if (!m(a))
                    return a;
                var u = l(a);
                if (u) {
                    if (t = i(a),
                    !b)
                        return e(a, t)
                } else {
                    var w = M.call(a)
                      , x = w == s;
                    if (w != v && w != n && (!x || p))
                        return K[w] ? j(a, w, b) : p ? a : {};
                    if (t = k(x ? {} : a),
                    !b)
                        return g(t, a)
                }
                q || (q = []),
                r || (r = []);
                for (var y = q.length; y--; )
                    if (q[y] == a)
                        return r[y];
                return q.push(a),
                r.push(t),
                (u ? f : h)(a, function(e, f) {
                    t[f] = d(e, b, c, f, a, q, r)
                }),
                t
            }
            var e = a(15)
              , f = a(16)
              , g = a(19)
              , h = a(26)
              , i = a(52)
              , j = a(53)
              , k = a(54)
              , l = a(68)
              , m = a(71)
              , n = "[object Arguments]"
              , o = "[object Array]"
              , p = "[object Boolean]"
              , q = "[object Date]"
              , r = "[object Error]"
              , s = "[object Function]"
              , t = "[object Map]"
              , u = "[object Number]"
              , v = "[object Object]"
              , w = "[object RegExp]"
              , x = "[object Set]"
              , y = "[object String]"
              , z = "[object WeakMap]"
              , A = "[object ArrayBuffer]"
              , B = "[object Float32Array]"
              , C = "[object Float64Array]"
              , D = "[object Int8Array]"
              , E = "[object Int16Array]"
              , F = "[object Int32Array]"
              , G = "[object Uint8Array]"
              , H = "[object Uint8ClampedArray]"
              , I = "[object Uint16Array]"
              , J = "[object Uint32Array]"
              , K = {};
            K[n] = K[o] = K[A] = K[p] = K[q] = K[B] = K[C] = K[D] = K[E] = K[F] = K[u] = K[v] = K[w] = K[y] = K[G] = K[H] = K[I] = K[J] = !0,
            K[r] = K[s] = K[t] = K[x] = K[z] = !1;
            var L = Object.prototype
              , M = L.toString;
            b.exports = d
        }
        , {
            15: 15,
            16: 16,
            19: 19,
            26: 26,
            52: 52,
            53: 53,
            54: 54,
            68: 68,
            71: 71
        }],
        22: [function(a, b, c) {
            function d(a, b, c) {
                c || (c = {});
                for (var d = -1, e = b.length; ++d < e; ) {
                    var f = b[d];
                    c[f] = a[f]
                }
                return c
            }
            b.exports = d
        }
        , {}],
        23: [function(a, b, c) {
            var d = a(26)
              , e = a(43)
              , f = e(d);
            b.exports = f
        }
        , {
            26: 26,
            43: 43
        }],
        24: [function(a, b, c) {
            var d = a(44)
              , e = d();
            b.exports = e
        }
        , {
            44: 44
        }],
        25: [function(a, b, c) {
            function d(a, b) {
                return e(a, b, f)
            }
            var e = a(24)
              , f = a(76);
            b.exports = d
        }
        , {
            24: 24,
            76: 76
        }],
        26: [function(a, b, c) {
            function d(a, b) {
                return e(a, b, f)
            }
            var e = a(24)
              , f = a(75);
            b.exports = d
        }
        , {
            24: 24,
            75: 75
        }],
        27: [function(a, b, c) {
            function d(a, b, c) {
                if (null  != a) {
                    void 0 !== c && c in e(a) && (b = [c]);
                    for (var d = 0, f = b.length; null  != a && f > d; )
                        a = a[b[d++]];
                    return d && d == f ? a : void 0
                }
            }
            var e = a(63);
            b.exports = d
        }
        , {
            63: 63
        }],
        28: [function(a, b, c) {
            function d(a, b, c, h, i, j) {
                return a === b ? !0 : null  == a || null  == b || !f(a) && !g(b) ? a !== a && b !== b : e(a, b, d, c, h, i, j)
            }
            var e = a(29)
              , f = a(71)
              , g = a(60);
            b.exports = d
        }
        , {
            29: 29,
            60: 60,
            71: 71
        }],
        29: [function(a, b, c) {
            function d(a, b, c, d, m, p, q) {
                var r = h(a)
                  , s = h(b)
                  , t = k
                  , u = k;
                r || (t = o.call(a),
                t == j ? t = l : t != l && (r = i(a))),
                s || (u = o.call(b),
                u == j ? u = l : u != l && (s = i(b)));
                var v = t == l
                  , w = u == l
                  , x = t == u;
                if (x && !r && !v)
                    return f(a, b, t);
                if (!m) {
                    var y = v && n.call(a, "__wrapped__")
                      , z = w && n.call(b, "__wrapped__");
                    if (y || z)
                        return c(y ? a.value() : a, z ? b.value() : b, d, m, p, q)
                }
                if (!x)
                    return !1;
                p || (p = []),
                q || (q = []);
                for (var A = p.length; A--; )
                    if (p[A] == a)
                        return q[A] == b;
                p.push(a),
                q.push(b);
                var B = (r ? e : g)(a, b, c, d, m, p, q);
                return p.pop(),
                q.pop(),
                B
            }
            var e = a(46)
              , f = a(47)
              , g = a(48)
              , h = a(68)
              , i = a(73)
              , j = "[object Arguments]"
              , k = "[object Array]"
              , l = "[object Object]"
              , m = Object.prototype
              , n = m.hasOwnProperty
              , o = m.toString;
            b.exports = d
        }
        , {
            46: 46,
            47: 47,
            48: 48,
            68: 68,
            73: 73
        }],
        30: [function(a, b, c) {
            function d(a, b, c) {
                var d = b.length
                  , g = d
                  , h = !c;
                if (null  == a)
                    return !g;
                for (a = f(a); d--; ) {
                    var i = b[d];
                    if (h && i[2] ? i[1] !== a[i[0]] : !(i[0] in a))
                        return !1
                }
                for (; ++d < g; ) {
                    i = b[d];
                    var j = i[0]
                      , k = a[j]
                      , l = i[1];
                    if (h && i[2]) {
                        if (void 0 === k && !(j in a))
                            return !1
                    } else {
                        var m = c ? c(k, l, j) : void 0;
                        if (!(void 0 === m ? e(l, k, c, !0) : m))
                            return !1
                    }
                }
                return !0
            }
            var e = a(28)
              , f = a(63);
            b.exports = d
        }
        , {
            28: 28,
            63: 63
        }],
        31: [function(a, b, c) {
            function d(a, b) {
                var c = -1
                  , d = f(a) ? Array(a.length) : [];
                return e(a, function(a, e, f) {
                    d[++c] = b(a, e, f)
                }),
                d
            }
            var e = a(23)
              , f = a(55);
            b.exports = d
        }
        , {
            23: 23,
            55: 55
        }],
        32: [function(a, b, c) {
            function d(a) {
                var b = f(a);
                if (1 == b.length && b[0][2]) {
                    var c = b[0][0]
                      , d = b[0][1];
                    return function(a) {
                        return null  == a ? !1 : a[c] === d && (void 0 !== d || c in g(a))
                    }
                }
                return function(a) {
                    return e(a, b)
                }
            }
            var e = a(30)
              , f = a(50)
              , g = a(63);
            b.exports = d
        }
        , {
            30: 30,
            50: 50,
            63: 63
        }],
        33: [function(a, b, c) {
            function d(a, b) {
                var c = h(a)
                  , d = i(a) && j(b)
                  , n = a + "";
                return a = m(a),
                function(h) {
                    if (null  == h)
                        return !1;
                    var i = n;
                    if (h = l(h),
                    (c || !d) && !(i in h)) {
                        if (h = 1 == a.length ? h : e(h, g(a, 0, -1)),
                        null  == h)
                            return !1;
                        i = k(a),
                        h = l(h)
                    }
                    return h[i] === b ? void 0 !== b || i in h : f(b, h[i], void 0, !0)
                }
            }
            var e = a(27)
              , f = a(28)
              , g = a(38)
              , h = a(68)
              , i = a(58)
              , j = a(61)
              , k = a(11)
              , l = a(63)
              , m = a(64);
            b.exports = d
        }
        , {
            11: 11,
            27: 27,
            28: 28,
            38: 38,
            58: 58,
            61: 61,
            63: 63,
            64: 64,
            68: 68
        }],
        34: [function(a, b, c) {
            function d(a, b, c, m, n) {
                if (!i(a))
                    return a;
                var o = h(b) && (g(b) || k(b))
                  , p = o ? void 0 : l(b);
                return e(p || b, function(e, g) {
                    if (p && (g = e,
                    e = b[g]),
                    j(e))
                        m || (m = []),
                        n || (n = []),
                        f(a, b, g, d, c, m, n);
                    else {
                        var h = a[g]
                          , i = c ? c(h, e, g, a, b) : void 0
                          , k = void 0 === i;
                        k && (i = e),
                        void 0 === i && (!o || g in a) || !k && (i === i ? i === h : h !== h) || (a[g] = i)
                    }
                }),
                a
            }
            var e = a(16)
              , f = a(35)
              , g = a(68)
              , h = a(55)
              , i = a(71)
              , j = a(60)
              , k = a(73)
              , l = a(75);
            b.exports = d
        }
        , {
            16: 16,
            35: 35,
            55: 55,
            60: 60,
            68: 68,
            71: 71,
            73: 73,
            75: 75
        }],
        35: [function(a, b, c) {
            function d(a, b, c, d, l, m, n) {
                for (var o = m.length, p = b[c]; o--; )
                    if (m[o] == p)
                        return void (a[c] = n[o]);
                var q = a[c]
                  , r = l ? l(q, p, c, a, b) : void 0
                  , s = void 0 === r;
                s && (r = p,
                h(p) && (g(p) || j(p)) ? r = g(q) ? q : h(q) ? e(q) : [] : i(p) || f(p) ? r = f(q) ? k(q) : i(q) ? q : {} : s = !1),
                m.push(p),
                n.push(r),
                s ? a[c] = d(r, p, l, m, n) : (r === r ? r !== q : q === q) && (a[c] = r)
            }
            var e = a(15)
              , f = a(67)
              , g = a(68)
              , h = a(55)
              , i = a(72)
              , j = a(73)
              , k = a(74);
            b.exports = d
        }
        , {
            15: 15,
            55: 55,
            67: 67,
            68: 68,
            72: 72,
            73: 73,
            74: 74
        }],
        36: [function(a, b, c) {
            function d(a) {
                return function(b) {
                    return null  == b ? void 0 : b[a]
                }
            }
            b.exports = d
        }
        , {}],
        37: [function(a, b, c) {
            function d(a) {
                var b = a + "";
                return a = f(a),
                function(c) {
                    return e(c, a, b)
                }
            }
            var e = a(27)
              , f = a(64);
            b.exports = d
        }
        , {
            27: 27,
            64: 64
        }],
        38: [function(a, b, c) {
            function d(a, b, c) {
                var d = -1
                  , e = a.length;
                b = null  == b ? 0 : +b || 0,
                0 > b && (b = -b > e ? 0 : e + b),
                c = void 0 === c || c > e ? e : +c || 0,
                0 > c && (c += e),
                e = b > c ? 0 : c - b >>> 0,
                b >>>= 0;
                for (var f = Array(e); ++d < e; )
                    f[d] = a[d + b];
                return f
            }
            b.exports = d
        }
        , {}],
        39: [function(a, b, c) {
            function d(a) {
                return null  == a ? "" : a + ""
            }
            b.exports = d
        }
        , {}],
        40: [function(a, b, c) {
            function d(a, b, c) {
                if ("function" != typeof a)
                    return e;
                if (void 0 === b)
                    return a;
                switch (c) {
                case 1:
                    return function(c) {
                        return a.call(b, c)
                    }
                    ;
                case 3:
                    return function(c, d, e) {
                        return a.call(b, c, d, e)
                    }
                    ;
                case 4:
                    return function(c, d, e, f) {
                        return a.call(b, c, d, e, f)
                    }
                    ;
                case 5:
                    return function(c, d, e, f, g) {
                        return a.call(b, c, d, e, f, g)
                    }
                }
                return function() {
                    return a.apply(b, arguments)
                }
            }
            var e = a(79);
            b.exports = d
        }
        , {
            79: 79
        }],
        41: [function(a, b, c) {
            (function(a) {
                function c(a) {
                    var b = new d(a.byteLength)
                      , c = new e(b);
                    return c.set(new e(a)),
                    b
                }
                var d = a.ArrayBuffer
                  , e = a.Uint8Array;
                b.exports = c
            }
            ).call(this, "undefined" != typeof global ? global : "undefined" != typeof self ? self : "undefined" != typeof window ? window : {})
        }
        , {}],
        42: [function(a, b, c) {
            function d(a) {
                return g(function(b, c) {
                    var d = -1
                      , g = null  == b ? 0 : c.length
                      , h = g > 2 ? c[g - 2] : void 0
                      , i = g > 2 ? c[2] : void 0
                      , j = g > 1 ? c[g - 1] : void 0;
                    for ("function" == typeof h ? (h = e(h, j, 5),
                    g -= 2) : (h = "function" == typeof j ? j : void 0,
                    g -= h ? 1 : 0),
                    i && f(c[0], c[1], i) && (h = 3 > g ? void 0 : h,
                    g = 1); ++d < g; ) {
                        var k = c[d];
                        k && a(b, k, h)
                    }
                    return b
                })
            }
            var e = a(40)
              , f = a(57)
              , g = a(14);
            b.exports = d
        }
        , {
            14: 14,
            40: 40,
            57: 57
        }],
        43: [function(a, b, c) {
            function d(a, b) {
                return function(c, d) {
                    var h = c ? e(c) : 0;
                    if (!f(h))
                        return a(c, d);
                    for (var i = b ? h : -1, j = g(c); (b ? i-- : ++i < h) && d(j[i], i, j) !== !1; )
                        ;
                    return c
                }
            }
            var e = a(49)
              , f = a(59)
              , g = a(63);
            b.exports = d
        }
        , {
            49: 49,
            59: 59,
            63: 63
        }],
        44: [function(a, b, c) {
            function d(a) {
                return function(b, c, d) {
                    for (var f = e(b), g = d(b), h = g.length, i = a ? h : -1; a ? i-- : ++i < h; ) {
                        var j = g[i];
                        if (c(f[j], j, f) === !1)
                            break
                    }
                    return b
                }
            }
            var e = a(63);
            b.exports = d
        }
        , {
            63: 63
        }],
        45: [function(a, b, c) {
            function d(a, b) {
                return function(c, d, g) {
                    return "function" == typeof d && void 0 === g && f(c) ? a(c, d) : b(c, e(d, g, 3))
                }
            }
            var e = a(40)
              , f = a(68);
            b.exports = d
        }
        , {
            40: 40,
            68: 68
        }],
        46: [function(a, b, c) {
            function d(a, b, c, d, f, g, h) {
                var i = -1
                  , j = a.length
                  , k = b.length;
                if (j != k && !(f && k > j))
                    return !1;
                for (; ++i < j; ) {
                    var l = a[i]
                      , m = b[i]
                      , n = d ? d(f ? m : l, f ? l : m, i) : void 0;
                    if (void 0 !== n) {
                        if (n)
                            continue;return !1
                    }
                    if (f) {
                        if (!e(b, function(a) {
                            return l === a || c(l, a, d, f, g, h)
                        }))
                            return !1
                    } else if (l !== m && !c(l, m, d, f, g, h))
                        return !1
                }
                return !0
            }
            var e = a(18);
            b.exports = d
        }
        , {
            18: 18
        }],
        47: [function(a, b, c) {
            function d(a, b, c) {
                switch (c) {
                case e:
                case f:
                    return +a == +b;
                case g:
                    return a.name == b.name && a.message == b.message;
                case h:
                    return a != +a ? b != +b : a == +b;
                case i:
                case j:
                    return a == b + ""
                }
                return !1
            }
            var e = "[object Boolean]"
              , f = "[object Date]"
              , g = "[object Error]"
              , h = "[object Number]"
              , i = "[object RegExp]"
              , j = "[object String]";
            b.exports = d
        }
        , {}],
        48: [function(a, b, c) {
            function d(a, b, c, d, f, h, i) {
                var j = e(a)
                  , k = j.length
                  , l = e(b)
                  , m = l.length;
                if (k != m && !f)
                    return !1;
                for (var n = k; n--; ) {
                    var o = j[n];
                    if (!(f ? o in b : g.call(b, o)))
                        return !1
                }
                for (var p = f; ++n < k; ) {
                    o = j[n];
                    var q = a[o]
                      , r = b[o]
                      , s = d ? d(f ? r : q, f ? q : r, o) : void 0;
                    if (!(void 0 === s ? c(q, r, d, f, h, i) : s))
                        return !1;
                    p || (p = "constructor" == o)
                }
                if (!p) {
                    var t = a.constructor
                      , u = b.constructor;
                    if (t != u && "constructor" in a && "constructor" in b && !("function" == typeof t && t instanceof t && "function" == typeof u && u instanceof u))
                        return !1
                }
                return !0
            }
            var e = a(75)
              , f = Object.prototype
              , g = f.hasOwnProperty;
            b.exports = d
        }
        , {
            75: 75
        }],
        49: [function(a, b, c) {
            var d = a(36)
              , e = d("length");
            b.exports = e
        }
        , {
            36: 36
        }],
        50: [function(a, b, c) {
            function d(a) {
                for (var b = f(a), c = b.length; c--; )
                    b[c][2] = e(b[c][1]);
                return b
            }
            var e = a(61)
              , f = a(78);
            b.exports = d
        }
        , {
            61: 61,
            78: 78
        }],
        51: [function(a, b, c) {
            function d(a, b) {
                var c = null  == a ? void 0 : a[b];
                return e(c) ? c : void 0
            }
            var e = a(70);
            b.exports = d
        }
        , {
            70: 70
        }],
        52: [function(a, b, c) {
            function d(a) {
                var b = a.length
                  , c = new a.constructor(b);
                return b && "string" == typeof a[0] && f.call(a, "index") && (c.index = a.index,
                c.input = a.input),
                c
            }
            var e = Object.prototype
              , f = e.hasOwnProperty;
            b.exports = d
        }
        , {}],
        53: [function(a, b, c) {
            function d(a, b, c) {
                var d = a.constructor;
                switch (b) {
                case k:
                    return e(a);
                case f:
                case g:
                    return new d(+a);
                case l:
                case m:
                case n:
                case o:
                case p:
                case q:
                case r:
                case s:
                case t:
                    var v = a.buffer;
                    return new d(c ? e(v) : v,a.byteOffset,a.length);
                case h:
                case j:
                    return new d(a);
                case i:
                    var w = new d(a.source,u.exec(a));
                    w.lastIndex = a.lastIndex
                }
                return w
            }
            var e = a(41)
              , f = "[object Boolean]"
              , g = "[object Date]"
              , h = "[object Number]"
              , i = "[object RegExp]"
              , j = "[object String]"
              , k = "[object ArrayBuffer]"
              , l = "[object Float32Array]"
              , m = "[object Float64Array]"
              , n = "[object Int8Array]"
              , o = "[object Int16Array]"
              , p = "[object Int32Array]"
              , q = "[object Uint8Array]"
              , r = "[object Uint8ClampedArray]"
              , s = "[object Uint16Array]"
              , t = "[object Uint32Array]"
              , u = /\w*$/;
            b.exports = d
        }
        , {
            41: 41
        }],
        54: [function(a, b, c) {
            function d(a) {
                var b = a.constructor;
                return "function" == typeof b && b instanceof b || (b = Object),
                new b
            }
            b.exports = d
        }
        , {}],
        55: [function(a, b, c) {
            function d(a) {
                return null  != a && f(e(a))
            }
            var e = a(49)
              , f = a(59);
            b.exports = d
        }
        , {
            49: 49,
            59: 59
        }],
        56: [function(a, b, c) {
            function d(a, b) {
                return a = "number" == typeof a || e.test(a) ? +a : -1,
                b = null  == b ? f : b,
                a > -1 && a % 1 == 0 && b > a
            }
            var e = /^\d+$/
              , f = 9007199254740991;
            b.exports = d
        }
        , {}],
        57: [function(a, b, c) {
            function d(a, b, c) {
                if (!g(c))
                    return !1;
                var d = typeof b;
                if ("number" == d ? e(c) && f(b, c.length) : "string" == d && b in c) {
                    var h = c[b];
                    return a === a ? a === h : h !== h
                }
                return !1
            }
            var e = a(55)
              , f = a(56)
              , g = a(71);
            b.exports = d
        }
        , {
            55: 55,
            56: 56,
            71: 71
        }],
        58: [function(a, b, c) {
            function d(a, b) {
                var c = typeof a;
                if ("string" == c && h.test(a) || "number" == c)
                    return !0;
                if (e(a))
                    return !1;
                var d = !g.test(a);
                return d || null  != b && a in f(b)
            }
            var e = a(68)
              , f = a(63)
              , g = /\.|\[(?:[^[\]]*|(["'])(?:(?!\1)[^\n\\]|\\.)*?\1)\]/
              , h = /^\w*$/;
            b.exports = d
        }
        , {
            63: 63,
            68: 68
        }],
        59: [function(a, b, c) {
            function d(a) {
                return "number" == typeof a && a > -1 && a % 1 == 0 && e >= a
            }
            var e = 9007199254740991;
            b.exports = d
        }
        , {}],
        60: [function(a, b, c) {
            function d(a) {
                return !!a && "object" == typeof a
            }
            b.exports = d
        }
        , {}],
        61: [function(a, b, c) {
            function d(a) {
                return a === a && !e(a)
            }
            var e = a(71);
            b.exports = d
        }
        , {
            71: 71
        }],
        62: [function(a, b, c) {
            function d(a) {
                for (var b = i(a), c = b.length, d = c && a.length, j = !!d && h(d) && (f(a) || e(a)), l = -1, m = []; ++l < c; ) {
                    var n = b[l];
                    (j && g(n, d) || k.call(a, n)) && m.push(n)
                }
                return m
            }
            var e = a(67)
              , f = a(68)
              , g = a(56)
              , h = a(59)
              , i = a(76)
              , j = Object.prototype
              , k = j.hasOwnProperty;
            b.exports = d
        }
        , {
            56: 56,
            59: 59,
            67: 67,
            68: 68,
            76: 76
        }],
        63: [function(a, b, c) {
            function d(a) {
                return e(a) ? a : Object(a)
            }
            var e = a(71);
            b.exports = d
        }
        , {
            71: 71
        }],
        64: [function(a, b, c) {
            function d(a) {
                if (f(a))
                    return a;
                var b = [];
                return e(a).replace(g, function(a, c, d, e) {
                    b.push(d ? e.replace(h, "$1") : c || a)
                }),
                b
            }
            var e = a(39)
              , f = a(68)
              , g = /[^.[\]]+|\[(?:(-?\d+(?:\.\d+)?)|(["'])((?:(?!\2)[^\n\\]|\\.)*?)\2)\]/g
              , h = /\\(\\)?/g;
            b.exports = d
        }
        , {
            39: 39,
            68: 68
        }],
        65: [function(a, b, c) {
            function d(a, b, c, d) {
                return b && "boolean" != typeof b && g(a, b, c) ? b = !1 : "function" == typeof b && (d = c,
                c = b,
                b = !1),
                "function" == typeof c ? e(a, b, f(c, d, 3)) : e(a, b)
            }
            var e = a(21)
              , f = a(40)
              , g = a(57);
            b.exports = d
        }
        , {
            21: 21,
            40: 40,
            57: 57
        }],
        66: [function(a, b, c) {
            function d(a, b, c) {
                return "function" == typeof b ? e(a, !0, f(b, c, 3)) : e(a, !0)
            }
            var e = a(21)
              , f = a(40);
            b.exports = d
        }
        , {
            21: 21,
            40: 40
        }],
        67: [function(a, b, c) {
            function d(a) {
                return f(a) && e(a) && h.call(a, "callee") && !i.call(a, "callee")
            }
            var e = a(55)
              , f = a(60)
              , g = Object.prototype
              , h = g.hasOwnProperty
              , i = g.propertyIsEnumerable;
            b.exports = d
        }
        , {
            55: 55,
            60: 60
        }],
        68: [function(a, b, c) {
            var d = a(51)
              , e = a(59)
              , f = a(60)
              , g = "[object Array]"
              , h = Object.prototype
              , i = h.toString
              , j = d(Array, "isArray")
              , k = j || function(a) {
                return f(a) && e(a.length) && i.call(a) == g
            }
            ;
            b.exports = k
        }
        , {
            51: 51,
            59: 59,
            60: 60
        }],
        69: [function(a, b, c) {
            function d(a) {
                return e(a) && h.call(a) == f
            }
            var e = a(71)
              , f = "[object Function]"
              , g = Object.prototype
              , h = g.toString;
            b.exports = d
        }
        , {
            71: 71
        }],
        70: [function(a, b, c) {
            function d(a) {
                return null  == a ? !1 : e(a) ? k.test(i.call(a)) : f(a) && g.test(a)
            }
            var e = a(69)
              , f = a(60)
              , g = /^\[object .+?Constructor\]$/
              , h = Object.prototype
              , i = Function.prototype.toString
              , j = h.hasOwnProperty
              , k = RegExp("^" + i.call(j).replace(/[\\^$.*+?()[\]{}|]/g, "\\$&").replace(/hasOwnProperty|(function).*?(?=\\\()| for .+?(?=\\\])/g, "$1.*?") + "$");
            b.exports = d
        }
        , {
            60: 60,
            69: 69
        }],
        71: [function(a, b, c) {
            function d(a) {
                var b = typeof a;
                return !!a && ("object" == b || "function" == b)
            }
            b.exports = d
        }
        , {}],
        72: [function(a, b, c) {
            function d(a) {
                var b;
                if (!g(a) || k.call(a) != h || f(a) || !j.call(a, "constructor") && (b = a.constructor,
                "function" == typeof b && !(b instanceof b)))
                    return !1;
                var c;
                return e(a, function(a, b) {
                    c = b
                }),
                void 0 === c || j.call(a, c)
            }
            var e = a(25)
              , f = a(67)
              , g = a(60)
              , h = "[object Object]"
              , i = Object.prototype
              , j = i.hasOwnProperty
              , k = i.toString;
            b.exports = d
        }
        , {
            25: 25,
            60: 60,
            67: 67
        }],
        73: [function(a, b, c) {
            function d(a) {
                return f(a) && e(a.length) && !!D[F.call(a)]
            }
            var e = a(59)
              , f = a(60)
              , g = "[object Arguments]"
              , h = "[object Array]"
              , i = "[object Boolean]"
              , j = "[object Date]"
              , k = "[object Error]"
              , l = "[object Function]"
              , m = "[object Map]"
              , n = "[object Number]"
              , o = "[object Object]"
              , p = "[object RegExp]"
              , q = "[object Set]"
              , r = "[object String]"
              , s = "[object WeakMap]"
              , t = "[object ArrayBuffer]"
              , u = "[object Float32Array]"
              , v = "[object Float64Array]"
              , w = "[object Int8Array]"
              , x = "[object Int16Array]"
              , y = "[object Int32Array]"
              , z = "[object Uint8Array]"
              , A = "[object Uint8ClampedArray]"
              , B = "[object Uint16Array]"
              , C = "[object Uint32Array]"
              , D = {};
            D[u] = D[v] = D[w] = D[x] = D[y] = D[z] = D[A] = D[B] = D[C] = !0,
            D[g] = D[h] = D[t] = D[i] = D[j] = D[k] = D[l] = D[m] = D[n] = D[o] = D[p] = D[q] = D[r] = D[s] = !1;
            var E = Object.prototype
              , F = E.toString;
            b.exports = d
        }
        , {
            59: 59,
            60: 60
        }],
        74: [function(a, b, c) {
            function d(a) {
                return e(a, f(a))
            }
            var e = a(22)
              , f = a(76);
            b.exports = d
        }
        , {
            22: 22,
            76: 76
        }],
        75: [function(a, b, c) {
            var d = a(51)
              , e = a(55)
              , f = a(71)
              , g = a(62)
              , h = d(Object, "keys")
              , i = h ? function(a) {
                var b = null  == a ? void 0 : a.constructor;
                return "function" == typeof b && b.prototype === a || "function" != typeof a && e(a) ? g(a) : f(a) ? h(a) : []
            }
             : g;
            b.exports = i
        }
        , {
            51: 51,
            55: 55,
            62: 62,
            71: 71
        }],
        76: [function(a, b, c) {
            function d(a) {
                if (null  == a)
                    return [];
                i(a) || (a = Object(a));
                var b = a.length;
                b = b && h(b) && (f(a) || e(a)) && b || 0;
                for (var c = a.constructor, d = -1, j = "function" == typeof c && c.prototype === a, l = Array(b), m = b > 0; ++d < b; )
                    l[d] = d + "";
                for (var n in a)
                    m && g(n, b) || "constructor" == n && (j || !k.call(a, n)) || l.push(n);
                return l
            }
            var e = a(67)
              , f = a(68)
              , g = a(56)
              , h = a(59)
              , i = a(71)
              , j = Object.prototype
              , k = j.hasOwnProperty;
            b.exports = d
        }
        , {
            56: 56,
            59: 59,
            67: 67,
            68: 68,
            71: 71
        }],
        77: [function(a, b, c) {
            var d = a(34)
              , e = a(42)
              , f = e(d);
            b.exports = f
        }
        , {
            34: 34,
            42: 42
        }],
        78: [function(a, b, c) {
            function d(a) {
                a = f(a);
                for (var b = -1, c = e(a), d = c.length, g = Array(d); ++b < d; ) {
                    var h = c[b];
                    g[b] = [h, a[h]]
                }
                return g
            }
            var e = a(75)
              , f = a(63);
            b.exports = d
        }
        , {
            63: 63,
            75: 75
        }],
        79: [function(a, b, c) {
            function d(a) {
                return a
            }
            b.exports = d
        }
        , {}],
        80: [function(a, b, c) {
            function d(a) {
                return g(a) ? e(a) : f(a)
            }
            var e = a(36)
              , f = a(37)
              , g = a(58);
            b.exports = d
        }
        , {
            36: 36,
            37: 37,
            58: 58
        }],
        81: [function(a, b, c) {
            "use strict";
            function d(b, c, d) {
                var f = a(6)("algoliasearch")
                  , g = a(65)
                  , h = a(68)
                  , i = a(13)
                  , j = "Usage: algoliasearch(applicationID, apiKey, opts)";
                if (!b)
                    throw new l.AlgoliaSearchError("Please provide an application ID. " + j);
                if (!c)
                    throw new l.AlgoliaSearchError("Please provide an API key. " + j);
                this.applicationID = b,
                this.apiKey = c;
                var k = [this.applicationID + "-1.algolianet.com", this.applicationID + "-2.algolianet.com", this.applicationID + "-3.algolianet.com"];
                this.hosts = {
                    read: [],
                    write: []
                },
                this.hostIndex = {
                    read: 0,
                    write: 0
                },
                d = d || {};
                var m = d.protocol || "https:"
                  , n = void 0 === d.timeout ? 2e3 : d.timeout;
                if (/:$/.test(m) || (m += ":"),
                "http:" !== d.protocol && "https:" !== d.protocol)
                    throw new l.AlgoliaSearchError("protocol must be `http:` or `https:` (was `" + d.protocol + "`)");
                d.hosts ? h(d.hosts) ? (this.hosts.read = g(d.hosts),
                this.hosts.write = g(d.hosts)) : (this.hosts.read = g(d.hosts.read),
                this.hosts.write = g(d.hosts.write)) : (this.hosts.read = [this.applicationID + "-dsn.algolia.net"].concat(k),
                this.hosts.write = [this.applicationID + ".algolia.net"].concat(k)),
                this.hosts.read = i(this.hosts.read, e(m)),
                this.hosts.write = i(this.hosts.write, e(m)),
                this.requestTimeout = n,
                this.extraHeaders = [],
                this.cache = d._cache || {},
                this._ua = d._ua,
                this._useCache = void 0 === d._useCache || d._cache ? !0 : d._useCache,
                this._setTimeout = d._setTimeout,
                f("init done, %j", this)
            }
            function e(a) {
                return function(b) {
                    return a + "//" + b.toLowerCase()
                }
            }
            function f() {
                var a = "Not implemented in this environment.\nIf you feel this is a mistake, write to support@algolia.com";
                throw new l.AlgoliaSearchError(a)
            }
            function g(a, b) {
                var c = a.toLowerCase().replace(".", "").replace("()", "");
                return "algoliasearch: `" + a + "` was replaced by `" + b + "`. Please see https://github.com/algolia/algoliasearch-client-js/wiki/Deprecated#" + c
            }
            function h(a, b) {
                b(a, 0)
            }
            function i(a, b) {
                function c() {
                    return d || (console.log(b),
                    d = !0),
                    a.apply(this, arguments)
                }
                var d = !1;
                return c
            }
            function j(a) {
                if (void 0 === Array.prototype.toJSON)
                    return JSON.stringify(a);
                var b = Array.prototype.toJSON;
                delete Array.prototype.toJSON;
                var c = JSON.stringify(a);
                return Array.prototype.toJSON = b,
                c
            }
            function k(a) {
                return function(b, c, d) {
                    if ("function" == typeof b && "object" == typeof c || "object" == typeof d)
                        throw new l.AlgoliaSearchError("index.search usage is index.search(query, params, cb)");
                    0 === arguments.length || "function" == typeof b ? (d = b,
                    b = "") : (1 === arguments.length || "function" == typeof c) && (d = c,
                    c = void 0),
                    "object" == typeof b && null  !== b ? (c = b,
                    b = void 0) : (void 0 === b || null  === b) && (b = "");
                    var e = "";
                    return void 0 !== b && (e += a + "=" + encodeURIComponent(b)),
                    void 0 !== c && (e = this.as._getSearchParams(c, e)),
                    this._search(e, d)
                }
            }
            b.exports = d;
            var l = a(87);
            d.prototype = {
                deleteIndex: function(a, b) {
                    return this._jsonRequest({
                        method: "DELETE",
                        url: "/1/indexes/" + encodeURIComponent(a),
                        hostType: "write",
                        callback: b
                    })
                },
                moveIndex: function(a, b, c) {
                    var d = {
                        operation: "move",
                        destination: b
                    };
                    return this._jsonRequest({
                        method: "POST",
                        url: "/1/indexes/" + encodeURIComponent(a) + "/operation",
                        body: d,
                        hostType: "write",
                        callback: c
                    })
                },
                copyIndex: function(a, b, c) {
                    var d = {
                        operation: "copy",
                        destination: b
                    };
                    return this._jsonRequest({
                        method: "POST",
                        url: "/1/indexes/" + encodeURIComponent(a) + "/operation",
                        body: d,
                        hostType: "write",
                        callback: c
                    })
                },
                getLogs: function(a, b, c) {
                    return 0 === arguments.length || "function" == typeof a ? (c = a,
                    a = 0,
                    b = 10) : (1 === arguments.length || "function" == typeof b) && (c = b,
                    b = 10),
                    this._jsonRequest({
                        method: "GET",
                        url: "/1/logs?offset=" + a + "&length=" + b,
                        hostType: "read",
                        callback: c
                    })
                },
                listIndexes: function(a, b) {
                    var c = "";
                    return void 0 === a || "function" == typeof a ? b = a : c = "?page=" + a,
                    this._jsonRequest({
                        method: "GET",
                        url: "/1/indexes" + c,
                        hostType: "read",
                        callback: b
                    })
                },
                initIndex: function(a) {
                    return new this.Index(this,a)
                },
                listUserKeys: function(a) {
                    return this._jsonRequest({
                        method: "GET",
                        url: "/1/keys",
                        hostType: "read",
                        callback: a
                    })
                },
                getUserKeyACL: function(a, b) {
                    return this._jsonRequest({
                        method: "GET",
                        url: "/1/keys/" + a,
                        hostType: "read",
                        callback: b
                    })
                },
                deleteUserKey: function(a, b) {
                    return this._jsonRequest({
                        method: "DELETE",
                        url: "/1/keys/" + a,
                        hostType: "write",
                        callback: b
                    })
                },
                addUserKey: function(b, c, d) {
                    var e = a(68)
                      , f = "Usage: client.addUserKey(arrayOfAcls[, params, callback])";
                    if (!e(b))
                        throw new Error(f);
                    (1 === arguments.length || "function" == typeof c) && (d = c,
                    c = null );
                    var g = {
                        acl: b
                    };
                    return c && (g.validity = c.validity,
                    g.maxQueriesPerIPPerHour = c.maxQueriesPerIPPerHour,
                    g.maxHitsPerQuery = c.maxHitsPerQuery,
                    g.indexes = c.indexes,
                    g.description = c.description,
                    c.queryParameters && (g.queryParameters = this._getSearchParams(c.queryParameters, "")),
                    g.referers = c.referers),
                    this._jsonRequest({
                        method: "POST",
                        url: "/1/keys",
                        body: g,
                        hostType: "write",
                        callback: d
                    })
                },
                addUserKeyWithValidity: i(function(a, b, c) {
                    return this.addUserKey(a, b, c)
                }, g("client.addUserKeyWithValidity()", "client.addUserKey()")),
                updateUserKey: function(b, c, d, e) {
                    var f = a(68)
                      , g = "Usage: client.updateUserKey(key, arrayOfAcls[, params, callback])";
                    if (!f(c))
                        throw new Error(g);
                    (2 === arguments.length || "function" == typeof d) && (e = d,
                    d = null );
                    var h = {
                        acl: c
                    };
                    return d && (h.validity = d.validity,
                    h.maxQueriesPerIPPerHour = d.maxQueriesPerIPPerHour,
                    h.maxHitsPerQuery = d.maxHitsPerQuery,
                    h.indexes = d.indexes,
                    h.description = d.description,
                    d.queryParameters && (h.queryParameters = this._getSearchParams(d.queryParameters, "")),
                    h.referers = d.referers),
                    this._jsonRequest({
                        method: "PUT",
                        url: "/1/keys/" + b,
                        body: h,
                        hostType: "write",
                        callback: e
                    })
                },
                setSecurityTags: function(a) {
                    if ("[object Array]" === Object.prototype.toString.call(a)) {
                        for (var b = [], c = 0; c < a.length; ++c)
                            if ("[object Array]" === Object.prototype.toString.call(a[c])) {
                                for (var d = [], e = 0; e < a[c].length; ++e)
                                    d.push(a[c][e]);
                                b.push("(" + d.join(",") + ")")
                            } else
                                b.push(a[c]);
                        a = b.join(",")
                    }
                    this.securityTags = a
                },
                setUserToken: function(a) {
                    this.userToken = a
                },
                startQueriesBatch: i(function() {
                    this._batch = []
                }, g("client.startQueriesBatch()", "client.search()")),
                addQueryInBatch: i(function(a, b, c) {
                    this._batch.push({
                        indexName: a,
                        query: b,
                        params: c
                    })
                }, g("client.addQueryInBatch()", "client.search()")),
                clearCache: function() {
                    this.cache = {}
                },
                sendQueriesBatch: i(function(a) {
                    return this.search(this._batch, a)
                }, g("client.sendQueriesBatch()", "client.search()")),
                setRequestTimeout: function(a) {
                    a && (this.requestTimeout = parseInt(a, 10))
                },
                search: function(b, c) {
                    var d = a(68)
                      , e = a(13)
                      , f = "Usage: client.search(arrayOfQueries[, callback])";
                    if (!d(b))
                        throw new Error(f);
                    var g = this
                      , h = {
                        requests: e(b, function(a) {
                            var b = "";
                            return void 0 !== a.query && (b += "query=" + encodeURIComponent(a.query)),
                            {
                                indexName: a.indexName,
                                params: g._getSearchParams(a.params, b)
                            }
                        })
                    }
                      , i = e(h.requests, function(a, b) {
                        return b + "=" + encodeURIComponent("/1/indexes/" + encodeURIComponent(a.indexName) + "?" + a.params)
                    }).join("&");
                    return this._jsonRequest({
                        cache: this.cache,
                        method: "POST",
                        url: "/1/indexes/*/queries",
                        body: h,
                        hostType: "read",
                        fallback: {
                            method: "GET",
                            url: "/1/indexes/*",
                            body: {
                                params: i
                            }
                        },
                        callback: c
                    })
                },
                batch: function(b, c) {
                    var d = a(68)
                      , e = "Usage: client.batch(operations[, callback])";
                    if (!d(b))
                        throw new Error(e);
                    return this._jsonRequest({
                        method: "POST",
                        url: "/1/indexes/*/batch",
                        body: {
                            requests: b
                        },
                        hostType: "write",
                        callback: c
                    })
                },
                destroy: f,
                enableRateLimitForward: f,
                disableRateLimitForward: f,
                useSecuredAPIKey: f,
                disableSecuredAPIKey: f,
                generateSecuredApiKey: f,
                Index: function(a, b) {
                    this.indexName = b,
                    this.as = a,
                    this.typeAheadArgs = null ,
                    this.typeAheadValueOption = null ,
                    this.cache = {}
                },
                setExtraHeader: function(a, b) {
                    this.extraHeaders.push({
                        name: a.toLowerCase(),
                        value: b
                    })
                },
                addAlgoliaAgent: function(a) {
                    this._ua += ";" + a
                },
                _jsonRequest: function(b) {
                    function c(a, h) {
                        function n(a) {
                            var b = a && a.body && a.body.message && a.body.status || a.statusCode || a && a.body && 200;
                            e("received response: statusCode: %s, computed statusCode: %d, headers: %j", a.statusCode, b, a.headers);
                            var c = 200 === b || 201 === b
                              , d = !c && 4 !== Math.floor(b / 100) && 1 !== Math.floor(b / 100);
                            if (g._useCache && c && f && (f[q] = a.responseText),
                            c)
                                return a.body;
                            if (d)
                                return i += 1,
                                p();
                            var h = new l.AlgoliaSearchError(a.body && a.body.message);
                            return g._promise.reject(h)
                        }
                        function o(d) {
                            return e("error: %s, stack: %s", d.message, d.stack),
                            d instanceof l.AlgoliaSearchError || (d = new l.Unknown(d && d.message,d)),
                            i += 1,
                            d instanceof l.Unknown || d instanceof l.UnparsableJSON || i >= g.hosts[b.hostType].length && (k || !m) ? g._promise.reject(d) : (g.hostIndex[b.hostType] = ++g.hostIndex[b.hostType] % g.hosts[b.hostType].length,
                            d instanceof l.RequestTimeout ? p() : (k || (i = 1 / 0),
                            c(a, h)))
                        }
                        function p() {
                            return g.hostIndex[b.hostType] = ++g.hostIndex[b.hostType] % g.hosts[b.hostType].length,
                            h.timeout = g.requestTimeout * (i + 1),
                            c(a, h)
                        }
                        var q;
                        if (g._useCache && (q = b.url),
                        g._useCache && d && (q += "_body_" + h.body),
                        g._useCache && f && void 0 !== f[q])
                            return e("serving response from cache"),
                            g._promise.resolve(JSON.parse(f[q]));
                        if (i >= g.hosts[b.hostType].length)
                            return !m || k ? (e("could not get any response"),
                            g._promise.reject(new l.AlgoliaSearchError("Cannot connect to the AlgoliaSearch API. Send an email to support@algolia.com to report and resolve the issue. Application id was: " + g.applicationID))) : (e("switching to fallback"),
                            i = 0,
                            h.method = b.fallback.method,
                            h.url = b.fallback.url,
                            h.jsonBody = b.fallback.body,
                            h.jsonBody && (h.body = j(h.jsonBody)),
                            h.timeout = g.requestTimeout * (i + 1),
                            g.hostIndex[b.hostType] = 0,
                            k = !0,
                            c(g._request.fallback, h));
                        var r = g.hosts[b.hostType][g.hostIndex[b.hostType]] + h.url
                          , s = {
                            body: h.body,
                            jsonBody: h.jsonBody,
                            method: h.method,
                            headers: g._computeRequestHeaders(),
                            timeout: h.timeout,
                            debug: e
                        };
                        return e("method: %s, url: %s, headers: %j, timeout: %d", s.method, r, s.headers, s.timeout),
                        a === g._request.fallback && e("using fallback"),
                        a.call(g, r, s).then(n, o)
                    }
                    var d, e = a(6)("algoliasearch:" + b.url), f = b.cache, g = this, i = 0, k = !1, m = g._request.fallback && b.fallback;
                    void 0 !== b.body && (d = j(b.body)),
                    e("request start");
                    var n = c(g._request, {
                        url: b.url,
                        method: b.method,
                        body: d,
                        jsonBody: b.body,
                        timeout: g.requestTimeout * (i + 1)
                    });
                    return b.callback ? void n.then(function(a) {
                        h(function() {
                            b.callback(null , a)
                        }, g._setTimeout || setTimeout)
                    }, function(a) {
                        h(function() {
                            b.callback(a)
                        }, g._setTimeout || setTimeout)
                    }) : n
                },
                _getSearchParams: function(a, b) {
                    if (void 0 === a || null  === a)
                        return b;
                    for (var c in a)
                        null  !== c && void 0 !== a[c] && a.hasOwnProperty(c) && (b += "" === b ? "" : "&",
                        b += c + "=" + encodeURIComponent("[object Array]" === Object.prototype.toString.call(a[c]) ? j(a[c]) : a[c]));
                    return b
                },
                _computeRequestHeaders: function() {
                    var b = a(12)
                      , c = {
                        "x-algolia-api-key": this.apiKey,
                        "x-algolia-application-id": this.applicationID,
                        "x-algolia-agent": this._ua
                    };
                    return this.userToken && (c["x-algolia-usertoken"] = this.userToken),
                    this.securityTags && (c["x-algolia-tagfilters"] = this.securityTags),
                    this.extraHeaders && b(this.extraHeaders, function(a) {
                        c[a.name] = a.value
                    }),
                    c
                }
            },
            d.prototype.Index.prototype = {
                clearCache: function() {
                    this.cache = {}
                },
                addObject: function(a, b, c) {
                    var d = this;
                    return (1 === arguments.length || "function" == typeof b) && (c = b,
                    b = void 0),
                    this.as._jsonRequest({
                        method: void 0 !== b ? "PUT" : "POST",
                        url: "/1/indexes/" + encodeURIComponent(d.indexName) + (void 0 !== b ? "/" + encodeURIComponent(b) : ""),
                        body: a,
                        hostType: "write",
                        callback: c
                    })
                },
                addObjects: function(b, c) {
                    var d = a(68)
                      , e = "Usage: index.addObjects(arrayOfObjects[, callback])";
                    if (!d(b))
                        throw new Error(e);
                    for (var f = this, g = {
                        requests: []
                    }, h = 0; h < b.length; ++h) {
                        var i = {
                            action: "addObject",
                            body: b[h]
                        };
                        g.requests.push(i)
                    }
                    return this.as._jsonRequest({
                        method: "POST",
                        url: "/1/indexes/" + encodeURIComponent(f.indexName) + "/batch",
                        body: g,
                        hostType: "write",
                        callback: c
                    })
                },
                getObject: function(a, b, c) {
                    var d = this;
                    (1 === arguments.length || "function" == typeof b) && (c = b,
                    b = void 0);
                    var e = "";
                    if (void 0 !== b) {
                        e = "?attributes=";
                        for (var f = 0; f < b.length; ++f)
                            0 !== f && (e += ","),
                            e += b[f]
                    }
                    return this.as._jsonRequest({
                        method: "GET",
                        url: "/1/indexes/" + encodeURIComponent(d.indexName) + "/" + encodeURIComponent(a) + e,
                        hostType: "read",
                        callback: c
                    })
                },
                getObjects: function(b, c, d) {
                    var e = a(68)
                      , f = a(13)
                      , g = "Usage: index.getObjects(arrayOfObjectIDs[, callback])";
                    if (!e(b))
                        throw new Error(g);
                    var h = this;
                    (1 === arguments.length || "function" == typeof c) && (d = c,
                    c = void 0);
                    var i = {
                        requests: f(b, function(a) {
                            var b = {
                                indexName: h.indexName,
                                objectID: a
                            };
                            return c && (b.attributesToRetrieve = c.join(",")),
                            b
                        })
                    };
                    return this.as._jsonRequest({
                        method: "POST",
                        url: "/1/indexes/*/objects",
                        hostType: "read",
                        body: i,
                        callback: d
                    })
                },
                partialUpdateObject: function(a, b, c) {
                    (1 === arguments.length || "function" == typeof b) && (c = b,
                    b = void 0);
                    var d = this
                      , e = "/1/indexes/" + encodeURIComponent(d.indexName) + "/" + encodeURIComponent(a.objectID) + "/partial";
                    return b === !1 && (e += "?createIfNotExists=false"),
                    this.as._jsonRequest({
                        method: "POST",
                        url: e,
                        body: a,
                        hostType: "write",
                        callback: c
                    })
                },
                partialUpdateObjects: function(b, c) {
                    var d = a(68)
                      , e = "Usage: index.partialUpdateObjects(arrayOfObjects[, callback])";
                    if (!d(b))
                        throw new Error(e);
                    for (var f = this, g = {
                        requests: []
                    }, h = 0; h < b.length; ++h) {
                        var i = {
                            action: "partialUpdateObject",
                            objectID: b[h].objectID,
                            body: b[h]
                        };
                        g.requests.push(i)
                    }
                    return this.as._jsonRequest({
                        method: "POST",
                        url: "/1/indexes/" + encodeURIComponent(f.indexName) + "/batch",
                        body: g,
                        hostType: "write",
                        callback: c
                    })
                },
                saveObject: function(a, b) {
                    var c = this;
                    return this.as._jsonRequest({
                        method: "PUT",
                        url: "/1/indexes/" + encodeURIComponent(c.indexName) + "/" + encodeURIComponent(a.objectID),
                        body: a,
                        hostType: "write",
                        callback: b
                    })
                },
                saveObjects: function(b, c) {
                    var d = a(68)
                      , e = "Usage: index.saveObjects(arrayOfObjects[, callback])";
                    if (!d(b))
                        throw new Error(e);
                    for (var f = this, g = {
                        requests: []
                    }, h = 0; h < b.length; ++h) {
                        var i = {
                            action: "updateObject",
                            objectID: b[h].objectID,
                            body: b[h]
                        };
                        g.requests.push(i)
                    }
                    return this.as._jsonRequest({
                        method: "POST",
                        url: "/1/indexes/" + encodeURIComponent(f.indexName) + "/batch",
                        body: g,
                        hostType: "write",
                        callback: c
                    })
                },
                deleteObject: function(a, b) {
                    if ("function" == typeof a || "string" != typeof a && "number" != typeof a) {
                        var c = new l.AlgoliaSearchError("Cannot delete an object without an objectID");
                        return b = a,
                        "function" == typeof b ? b(c) : this.as._promise.reject(c)
                    }
                    var d = this;
                    return this.as._jsonRequest({
                        method: "DELETE",
                        url: "/1/indexes/" + encodeURIComponent(d.indexName) + "/" + encodeURIComponent(a),
                        hostType: "write",
                        callback: b
                    })
                },
                deleteObjects: function(b, c) {
                    var d = a(68)
                      , e = a(13)
                      , f = "Usage: index.deleteObjects(arrayOfObjectIDs[, callback])";
                    if (!d(b))
                        throw new Error(f);
                    var g = this
                      , h = {
                        requests: e(b, function(a) {
                            return {
                                action: "deleteObject",
                                objectID: a,
                                body: {
                                    objectID: a
                                }
                            }
                        })
                    };
                    return this.as._jsonRequest({
                        method: "POST",
                        url: "/1/indexes/" + encodeURIComponent(g.indexName) + "/batch",
                        body: h,
                        hostType: "write",
                        callback: c
                    })
                },
                deleteByQuery: function(b, c, d) {
                    function e(a) {
                        if (0 === a.nbHits)
                            return a;
                        var b = l(a.hits, function(a) {
                            return a.objectID
                        });
                        return m.deleteObjects(b).then(f).then(g)
                    }
                    function f(a) {
                        return m.waitTask(a.taskID)
                    }
                    function g() {
                        return m.deleteByQuery(b, c)
                    }
                    function i() {
                        h(function() {
                            d(null )
                        }, n._setTimeout || setTimeout)
                    }
                    function j(a) {
                        h(function() {
                            d(a)
                        }, n._setTimeout || setTimeout)
                    }
                    var k = a(65)
                      , l = a(13)
                      , m = this
                      , n = m.as;
                    1 === arguments.length || "function" == typeof c ? (d = c,
                    c = {}) : c = k(c),
                    c.attributesToRetrieve = "objectID",
                    c.hitsPerPage = 1e3,
                    c.distinct = !1,
                    this.clearCache();
                    var o = this.search(b, c).then(e);
                    return d ? void o.then(i, j) : o
                },
                search: k("query"),
                similarSearch: k("similarQuery"),
                browse: function(b, c, d) {
                    var e, f, g = a(77), h = this;
                    0 === arguments.length || 1 === arguments.length && "function" == typeof arguments[0] ? (e = 0,
                    d = arguments[0],
                    b = void 0) : "number" == typeof arguments[0] ? (e = arguments[0],
                    "number" == typeof arguments[1] ? f = arguments[1] : "function" == typeof arguments[1] && (d = arguments[1],
                    f = void 0),
                    b = void 0,
                    c = void 0) : "object" == typeof arguments[0] ? ("function" == typeof arguments[1] && (d = arguments[1]),
                    c = arguments[0],
                    b = void 0) : "string" == typeof arguments[0] && "function" == typeof arguments[1] && (d = arguments[1],
                    c = void 0),
                    c = g({}, c || {}, {
                        page: e,
                        hitsPerPage: f,
                        query: b
                    });
                    var i = this.as._getSearchParams(c, "");
                    return this.as._jsonRequest({
                        method: "GET",
                        url: "/1/indexes/" + encodeURIComponent(h.indexName) + "/browse?" + i,
                        hostType: "read",
                        callback: d
                    })
                },
                browseFrom: function(a, b) {
                    return this.as._jsonRequest({
                        method: "GET",
                        url: "/1/indexes/" + encodeURIComponent(this.indexName) + "/browse?cursor=" + encodeURIComponent(a),
                        hostType: "read",
                        callback: b
                    })
                },
                browseAll: function(b, c) {
                    function d(a) {
                        if (!h._stopped) {
                            var b;
                            b = void 0 !== a ? "cursor=" + encodeURIComponent(a) : k,
                            i._jsonRequest({
                                method: "GET",
                                url: "/1/indexes/" + encodeURIComponent(j.indexName) + "/browse?" + b,
                                hostType: "read",
                                callback: e
                            })
                        }
                    }
                    function e(a, b) {
                        return h._stopped ? void 0 : a ? void h._error(a) : (h._result(b),
                        void 0 === b.cursor ? void h._end() : void d(b.cursor))
                    }
                    "object" == typeof b && (c = b,
                    b = void 0);
                    var f = a(77)
                      , g = a(82)
                      , h = new g
                      , i = this.as
                      , j = this
                      , k = i._getSearchParams(f({}, c || {}, {
                        query: b
                    }), "");
                    return d(),
                    h
                },
                ttAdapter: function(a) {
                    var b = this;
                    return function(c, d, e) {
                        var f;
                        f = "function" == typeof e ? e : d,
                        b.search(c, a, function(a, b) {
                            return a ? void f(a) : void f(b.hits)
                        })
                    }
                },
                waitTask: function(a, b) {
                    function c() {
                        return k._jsonRequest({
                            method: "GET",
                            hostType: "read",
                            url: "/1/indexes/" + encodeURIComponent(j.indexName) + "/task/" + a
                        }).then(function(a) {
                            i++;
                            var b = f * i * i;
                            return b > g && (b = g),
                            "published" !== a.status ? k._promise.delay(b).then(c) : a
                        })
                    }
                    function d(a) {
                        h(function() {
                            b(null , a)
                        }, k._setTimeout || setTimeout)
                    }
                    function e(a) {
                        h(function() {
                            b(a)
                        }, k._setTimeout || setTimeout)
                    }
                    var f = 100
                      , g = 5e3
                      , i = 0
                      , j = this
                      , k = j.as
                      , l = c();
                    return b ? void l.then(d, e) : l
                },
                clearIndex: function(a) {
                    var b = this;
                    return this.as._jsonRequest({
                        method: "POST",
                        url: "/1/indexes/" + encodeURIComponent(b.indexName) + "/clear",
                        hostType: "write",
                        callback: a
                    })
                },
                getSettings: function(a) {
                    var b = this;
                    return this.as._jsonRequest({
                        method: "GET",
                        url: "/1/indexes/" + encodeURIComponent(b.indexName) + "/settings",
                        hostType: "read",
                        callback: a
                    })
                },
                setSettings: function(a, b) {
                    var c = this;
                    return this.as._jsonRequest({
                        method: "PUT",
                        url: "/1/indexes/" + encodeURIComponent(c.indexName) + "/settings",
                        hostType: "write",
                        body: a,
                        callback: b
                    })
                },
                listUserKeys: function(a) {
                    var b = this;
                    return this.as._jsonRequest({
                        method: "GET",
                        url: "/1/indexes/" + encodeURIComponent(b.indexName) + "/keys",
                        hostType: "read",
                        callback: a
                    })
                },
                getUserKeyACL: function(a, b) {
                    var c = this;
                    return this.as._jsonRequest({
                        method: "GET",
                        url: "/1/indexes/" + encodeURIComponent(c.indexName) + "/keys/" + a,
                        hostType: "read",
                        callback: b
                    })
                },
                deleteUserKey: function(a, b) {
                    var c = this;
                    return this.as._jsonRequest({
                        method: "DELETE",
                        url: "/1/indexes/" + encodeURIComponent(c.indexName) + "/keys/" + a,
                        hostType: "write",
                        callback: b
                    })
                },
                addUserKey: function(b, c, d) {
                    var e = a(68)
                      , f = "Usage: index.addUserKey(arrayOfAcls[, params, callback])";
                    if (!e(b))
                        throw new Error(f);
                    (1 === arguments.length || "function" == typeof c) && (d = c,
                    c = null );
                    var g = {
                        acl: b
                    };
                    return c && (g.validity = c.validity,
                    g.maxQueriesPerIPPerHour = c.maxQueriesPerIPPerHour,
                    g.maxHitsPerQuery = c.maxHitsPerQuery,
                    g.description = c.description,
                    c.queryParameters && (g.queryParameters = this.as._getSearchParams(c.queryParameters, "")),
                    g.referers = c.referers),
                    this.as._jsonRequest({
                        method: "POST",
                        url: "/1/indexes/" + encodeURIComponent(this.indexName) + "/keys",
                        body: g,
                        hostType: "write",
                        callback: d
                    })
                },
                addUserKeyWithValidity: i(function(a, b, c) {
                    return this.addUserKey(a, b, c)
                }, g("index.addUserKeyWithValidity()", "index.addUserKey()")),
                updateUserKey: function(b, c, d, e) {
                    var f = a(68)
                      , g = "Usage: index.updateUserKey(key, arrayOfAcls[, params, callback])";
                    if (!f(c))
                        throw new Error(g);
                    (2 === arguments.length || "function" == typeof d) && (e = d,
                    d = null );
                    var h = {
                        acl: c
                    };
                    return d && (h.validity = d.validity,
                    h.maxQueriesPerIPPerHour = d.maxQueriesPerIPPerHour,
                    h.maxHitsPerQuery = d.maxHitsPerQuery,
                    h.description = d.description,
                    d.queryParameters && (h.queryParameters = this.as._getSearchParams(d.queryParameters, "")),
                    h.referers = d.referers),
                    this.as._jsonRequest({
                        method: "PUT",
                        url: "/1/indexes/" + encodeURIComponent(this.indexName) + "/keys/" + b,
                        body: h,
                        hostType: "write",
                        callback: e
                    })
                },
                _search: function(a, b) {
                    return this.as._jsonRequest({
                        cache: this.cache,
                        method: "POST",
                        url: "/1/indexes/" + encodeURIComponent(this.indexName) + "/query",
                        body: {
                            params: a
                        },
                        hostType: "read",
                        fallback: {
                            method: "GET",
                            url: "/1/indexes/" + encodeURIComponent(this.indexName),
                            body: {
                                params: a
                            }
                        },
                        callback: b
                    })
                },
                as: null ,
                indexName: null ,
                typeAheadArgs: null ,
                typeAheadValueOption: null 
            }
        }
        , {
            12: 12,
            13: 13,
            6: 6,
            65: 65,
            68: 68,
            77: 77,
            82: 82,
            87: 87
        }],
        82: [function(a, b, c) {
            "use strict";
            function d() {}
            b.exports = d;
            var e = a(10)
              , f = a(1).EventEmitter;
            e(d, f),
            d.prototype.stop = function() {
                this._stopped = !0,
                this._clean()
            }
            ,
            d.prototype._end = function() {
                this.emit("end"),
                this._clean()
            }
            ,
            d.prototype._error = function(a) {
                this.emit("error", a),
                this._clean()
            }
            ,
            d.prototype._result = function(a) {
                this.emit("result", a)
            }
            ,
            d.prototype._clean = function() {
                this.removeAllListeners("stop"),
                this.removeAllListeners("end"),
                this.removeAllListeners("error"),
                this.removeAllListeners("result")
            }
        }
        , {
            1: 1,
            10: 10
        }],
        83: [function(a, b, c) {
            "use strict";
            function d(b, c, f) {
                var g = a(66)
                  , h = a(84);
                return f = g(f || {}),
                void 0 === f.protocol && (f.protocol = h()),
                f._ua = f._ua || d.ua,
                new e(b,c,f)
            }
            function e() {
                h.apply(this, arguments)
            }
            b.exports = d;
            var f = a(10)
              , g = window.Promise || a(9).Promise
              , h = a(81)
              , i = a(87)
              , j = a(85)
              , k = a(86);
            d.version = a(88),
            d.ua = "Algolia for vanilla JavaScript " + d.version,
            window.__algolia = {
                debug: a(6),
                algoliasearch: d
            };
            var l = {
                hasXMLHttpRequest: "XMLHttpRequest" in window,
                hasXDomainRequest: "XDomainRequest" in window,
                cors: "withCredentials" in new XMLHttpRequest,
                timeout: "timeout" in new XMLHttpRequest
            };
            f(e, h),
            e.prototype._request = function(a, b) {
                return new g(function(c, d) {
                    function e() {
                        if (!k) {
                            l.timeout || clearTimeout(h);
                            var a;
                            try {
                                a = {
                                    body: JSON.parse(n.responseText),
                                    responseText: n.responseText,
                                    statusCode: n.status,
                                    headers: n.getAllResponseHeaders && n.getAllResponseHeaders() || {}
                                }
                            } catch (b) {
                                a = new i.UnparsableJSON({
                                    more: n.responseText
                                })
                            }
                            a instanceof i.UnparsableJSON ? d(a) : c(a)
                        }
                    }
                    function f(a) {
                        k || (l.timeout || clearTimeout(h),
                        d(new i.Network({
                            more: a
                        })))
                    }
                    function g() {
                        l.timeout || (k = !0,
                        n.abort()),
                        d(new i.RequestTimeout)
                    }
                    if (!l.cors && !l.hasXDomainRequest)
                        return void d(new i.Network("CORS not supported"));
                    a = j(a, b.headers);
                    var h, k, m = b.body, n = l.cors ? new XMLHttpRequest : new XDomainRequest;
                    n instanceof XMLHttpRequest ? n.open(b.method, a, !0) : n.open(b.method, a),
                    l.cors && (m && ("POST" === b.method ? n.setRequestHeader("content-type", "application/x-www-form-urlencoded") : n.setRequestHeader("content-type", "application/json")),
                    n.setRequestHeader("accept", "application/json")),
                    n.onprogress = function() {}
                    ,
                    n.onload = e,
                    n.onerror = f,
                    l.timeout ? (n.timeout = b.timeout,
                    n.ontimeout = g) : h = setTimeout(g, b.timeout),
                    n.send(m)
                }
                )
            }
            ,
            e.prototype._request.fallback = function(a, b) {
                return a = j(a, b.headers),
                new g(function(c, d) {
                    k(a, b, function(a, b) {
                        return a ? void d(a) : void c(b)
                    })
                }
                )
            }
            ,
            e.prototype._promise = {
                reject: function(a) {
                    return g.reject(a)
                },
                resolve: function(a) {
                    return g.resolve(a)
                },
                delay: function(a) {
                    return new g(function(b) {
                        setTimeout(b, a)
                    }
                    )
                }
            }
        }
        , {
            10: 10,
            6: 6,
            66: 66,
            81: 81,
            84: 84,
            85: 85,
            86: 86,
            87: 87,
            88: 88,
            9: 9
        }],
        84: [function(a, b, c) {
            "use strict";
            function d() {
                var a = window.document.location.protocol;
                return "http:" !== a && "https:" !== a && (a = "http:"),
                a
            }
            b.exports = d
        }
        , {}],
        85: [function(a, b, c) {
            "use strict";
            function d(a, b) {
                return a += /\?/.test(a) ? "&" : "?",
                a + e.encode(b)
            }
            b.exports = d;
            var e = a(5)
        }
        , {
            5: 5
        }],
        86: [function(a, b, c) {
            "use strict";
            function d(a, b, c) {
                function d() {
                    b.debug("JSONP: success"),
                    p || l || (p = !0,
                    k || (b.debug("JSONP: Fail. Script loaded but did not call the callback"),
                    h(),
                    c(new e.JSONPScriptFail)))
                }
                function g() {
                    ("loaded" === this.readyState || "complete" === this.readyState) && d()
                }
                function h() {
                    clearTimeout(q),
                    n.onload = null ,
                    n.onreadystatechange = null ,
                    n.onerror = null ,
                    m.removeChild(n);
                    try {
                        delete window[o],
                        delete window[o + "_loaded"]
                    } catch (a) {
                        window[o] = null ,
                        window[o + "_loaded"] = null 
                    }
                }
                function i() {
                    b.debug("JSONP: Script timeout"),
                    l = !0,
                    h(),
                    c(new e.RequestTimeout)
                }
                function j() {
                    b.debug("JSONP: Script error"),
                    p || l || (h(),
                    c(new e.JSONPScriptError))
                }
                if ("GET" !== b.method)
                    return void c(new Error("Method " + b.method + " " + a + " is not supported by JSONP."));
                b.debug("JSONP: start");
                var k = !1
                  , l = !1;
                f += 1;
                var m = document.getElementsByTagName("head")[0]
                  , n = document.createElement("script")
                  , o = "algoliaJSONP_" + f
                  , p = !1;
                window[o] = function(a) {
                    try {
                        delete window[o]
                    } catch (b) {
                        window[o] = void 0
                    }
                    l || (k = !0,
                    h(),
                    c(null , {
                        body: a
                    }))
                }
                ,
                a += "&callback=" + o,
                b.jsonBody && b.jsonBody.params && (a += "&" + b.jsonBody.params);
                var q = setTimeout(i, b.timeout);
                n.onreadystatechange = g,
                n.onload = d,
                n.onerror = j,
                n.async = !0,
                n.defer = !0,
                n.src = a,
                m.appendChild(n)
            }
            b.exports = d;
            var e = a(87)
              , f = 0
        }
        , {
            87: 87
        }],
        87: [function(a, b, c) {
            "use strict";
            function d(b, c) {
                var d = a(12)
                  , e = this;
                "function" == typeof Error.captureStackTrace ? Error.captureStackTrace(this, this.constructor) : e.stack = (new Error).stack || "Cannot get a stacktrace, browser is too old",
                this.name = this.constructor.name,
                this.message = b || "Unknown error",
                c && d(c, function(a, b) {
                    e[b] = a
                })
            }
            function e(a, b) {
                function c() {
                    var c = Array.prototype.slice.call(arguments, 0);
                    "string" != typeof c[0] && c.unshift(b),
                    d.apply(this, c),
                    this.name = "AlgoliaSearch" + a + "Error"
                }
                return f(c, d),
                c
            }
            var f = a(10);
            f(d, Error),
            b.exports = {
                AlgoliaSearchError: d,
                UnparsableJSON: e("UnparsableJSON", "Could not parse the incoming response as JSON, see err.more for details"),
                RequestTimeout: e("RequestTimeout", "Request timedout before getting a response"),
                Network: e("Network", "Network issue, see err.more for details"),
                JSONPScriptFail: e("JSONPScriptFail", "<script> was loaded but did not call our provided callback"),
                JSONPScriptError: e("JSONPScriptError", "<script> unable to load due to an `error` event on it"),
                Unknown: e("Unknown", "Unknown error occured")
            }
        }
        , {
            10: 10,
            12: 12
        }],
        88: [function(a, b, c) {
            "use strict";
            b.exports = "3.11.0"
        }
        , {}]
    }, {}, [83])(83)
}),
!function(a) {
    if ("object" == typeof exports && "undefined" != typeof module)
        module.exports = a();
    else if ("function" == typeof define && define.amd)
        define([], a);
    else {
        var b;
        b = "undefined" != typeof window ? window : "undefined" != typeof global ? global : "undefined" != typeof self ? self : this,
        b.Raven = a()
    }
}(function() {
    return function a(b, c, d) {
        function e(g, h) {
            if (!c[g]) {
                if (!b[g]) {
                    var i = "function" == typeof require && require;
                    if (!h && i)
                        return i(g, !0);
                    if (f)
                        return f(g, !0);
                    var j = new Error("Cannot find module '" + g + "'");
                    throw j.code = "MODULE_NOT_FOUND",
                    j
                }
                var k = c[g] = {
                    exports: {}
                };
                b[g][0].call(k.exports, function(a) {
                    var c = b[g][1][a];
                    return e(c ? c : a)
                }, k, k.exports, a, b, c, d)
            }
            return c[g].exports
        }
        for (var f = "function" == typeof require && require, g = 0; d.length > g; g++)
            e(d[g]);
        return e
    }({
        1: [function(a, b, c) {
            "use strict";
            function d(a) {
                this.name = "RavenConfigError",
                this.message = a
            }
            d.prototype = new Error,
            d.prototype.constructor = d,
            b.exports = d
        }
        , {}],
        2: [function(a, b, c) {
            "use strict";
            function d() {
                return +new Date
            }
            function e() {
                this.a = !("object" != typeof JSON || !JSON.stringify),
                this.b = "undefined" != typeof document,
                this.c = null ,
                this.d = null ,
                this.e = null ,
                this.f = null ,
                this.g = null ,
                this.h = {},
                this.i = {
                    logger: "javascript",
                    ignoreErrors: [],
                    ignoreUrls: [],
                    whitelistUrls: [],
                    includePaths: [],
                    crossOrigin: "anonymous",
                    collectWindowErrors: !0,
                    maxMessageLength: 0,
                    stackTraceLimit: 50
                },
                this.j = 0,
                this.k = !1,
                this.l = Error.stackTraceLimit,
                this.m = window.console || {},
                this.n = {},
                this.o = [],
                this.p = d(),
                this.q = [];
                for (var a in this.m)
                    this.n[a] = this.m[a]
            }
            var f = a(5)
              , g = a(1)
              , h = a(4)
              , i = h.isFunction
              , j = h.isUndefined
              , k = h.isError
              , l = h.isEmptyObject
              , m = h.hasKey
              , n = h.joinRegExp
              , o = h.each
              , p = h.objectMerge
              , q = h.truncate
              , r = h.urlencode
              , s = h.uuid4
              , t = "source protocol user pass host port path".split(" ")
              , u = /^(?:(\w+):)?\/\/(?:(\w+)(:\w+)?@)?([\w\.-]+)(?::(\d+))?(\/.*)/;
            e.prototype = {
                VERSION: "2.3.0",
                debug: !1,
                TraceKit: f,
                config: function(a, b) {
                    var c = this;
                    if (this.e)
                        return this.r("error", "Error: Raven has already been configured"),
                        this;
                    if (!a)
                        return this;
                    b && o(b, function(a, b) {
                        "tags" === a || "extra" === a ? c.h[a] = b : c.i[a] = b
                    });
                    var d = this.s(a)
                      , e = d.path.lastIndexOf("/")
                      , g = d.path.substr(1, e);
                    return this.t = a,
                    this.i.ignoreErrors.push(/^Script error\.?$/),
                    this.i.ignoreErrors.push(/^Javascript error: Script error\.? on line 0$/),
                    this.i.ignoreErrors = n(this.i.ignoreErrors),
                    this.i.ignoreUrls = this.i.ignoreUrls.length ? n(this.i.ignoreUrls) : !1,
                    this.i.whitelistUrls = this.i.whitelistUrls.length ? n(this.i.whitelistUrls) : !1,
                    this.i.includePaths = n(this.i.includePaths),
                    this.f = d.user,
                    this.u = d.pass && d.pass.substr(1),
                    this.g = d.path.substr(e + 1),
                    this.e = this.v(d),
                    this.w = this.e + "/" + g + "api/" + this.g + "/store/",
                    this.i.fetchContext && (f.remoteFetching = !0),
                    this.i.linesOfContext && (f.linesOfContext = this.i.linesOfContext),
                    f.collectWindowErrors = !!this.i.collectWindowErrors,
                    this
                },
                install: function() {
                    var a = this;
                    return this.isSetup() && !this.k && (f.report.subscribe(function() {
                        a.x.apply(a, arguments)
                    }),
                    this.y(),
                    this.z(),
                    this.k = !0),
                    Error.stackTraceLimit = this.i.stackTraceLimit,
                    this
                },
                context: function(a, b, c) {
                    return i(a) && (c = b || [],
                    b = a,
                    a = void 0),
                    this.wrap(a, b).apply(this, c)
                },
                wrap: function(a, b) {
                    function c() {
                        for (var c = [], e = arguments.length, f = !a || a && a.deep !== !1; e--; )
                            c[e] = f ? d.wrap(a, arguments[e]) : arguments[e];
                        try {
                            return b.apply(this, c)
                        } catch (g) {
                            throw d.A(),
                            d.captureException(g, a),
                            g
                        }
                    }
                    var d = this;
                    if (j(b) && !i(a))
                        return a;
                    if (i(a) && (b = a,
                    a = void 0),
                    !i(b))
                        return b;
                    try {
                        if (b.B)
                            return b
                    } catch (e) {
                        return b
                    }
                    if (b.C)
                        return b.C;
                    for (var f in b)
                        m(b, f) && (c[f] = b[f]);
                    return b.C = c,
                    c.prototype = b.prototype,
                    c.B = !0,
                    c.D = b,
                    c
                },
                uninstall: function() {
                    return f.report.uninstall(),
                    this.E(),
                    Error.stackTraceLimit = this.l,
                    this.k = !1,
                    this
                },
                captureException: function(a, b) {
                    if (!k(a))
                        return this.captureMessage(a, b);
                    this.c = a;
                    try {
                        var c = f.computeStackTrace(a);
                        this.F(c, b)
                    } catch (d) {
                        if (a !== d)
                            throw d
                    }
                    return this
                },
                captureMessage: function(a, b) {
                    return this.i.ignoreErrors.test && this.i.ignoreErrors.test(a) ? void 0 : (this.G(p({
                        message: a + ""
                    }, b)),
                    this)
                },
                addPlugin: function(a) {
                    var b = Array.prototype.slice.call(arguments, 1);
                    return this.o.push([a, b]),
                    this.k && this.z(),
                    this
                },
                setUserContext: function(a) {
                    return this.h.user = a,
                    this
                },
                setExtraContext: function(a) {
                    return this.H("extra", a),
                    this
                },
                setTagsContext: function(a) {
                    return this.H("tags", a),
                    this
                },
                clearContext: function() {
                    return this.h = {},
                    this
                },
                getContext: function() {
                    return JSON.parse(JSON.stringify(this.h))
                },
                setRelease: function(a) {
                    return this.i.release = a,
                    this
                },
                setDataCallback: function(a) {
                    return this.i.dataCallback = a,
                    this
                },
                setShouldSendCallback: function(a) {
                    return this.i.shouldSendCallback = a,
                    this
                },
                setTransport: function(a) {
                    return this.i.transport = a,
                    this
                },
                lastException: function() {
                    return this.c
                },
                lastEventId: function() {
                    return this.d
                },
                isSetup: function() {
                    return this.a ? this.e ? !0 : (this.ravenNotConfiguredError || (this.ravenNotConfiguredError = !0,
                    this.r("error", "Error: Raven has not been configured.")),
                    !1) : !1
                },
                afterLoad: function() {
                    var a = window.RavenConfig;
                    a && this.config(a.dsn, a.config).install()
                },
                showReportDialog: function(a) {
                    if (window.document) {
                        a = a || {};
                        var b = a.eventId || this.lastEventId();
                        if (!b)
                            throw new g("Missing eventId");
                        var c = a.dsn || this.t;
                        if (!c)
                            throw new g("Missing DSN");
                        var d = encodeURIComponent
                          , e = "";
                        e += "?eventId=" + d(b),
                        e += "&dsn=" + d(c);
                        var f = a.user || this.h.user;
                        f && (f.name && (e += "&name=" + d(f.name)),
                        f.email && (e += "&email=" + d(f.email)));
                        var h = this.v(this.s(c))
                          , i = document.createElement("script");
                        i.async = !0,
                        i.src = h + "/api/embed/error-page/" + e,
                        (document.head || document.body).appendChild(i)
                    }
                },
                A: function() {
                    var a = this;
                    this.j += 1,
                    setTimeout(function() {
                        a.j -= 1
                    })
                },
                I: function(a, b) {
                    var c, d;
                    if (this.b) {
                        b = b || {},
                        a = "raven" + a.substr(0, 1).toUpperCase() + a.substr(1),
                        document.createEvent ? (c = document.createEvent("HTMLEvents"),
                        c.initEvent(a, !0, !0)) : (c = document.createEventObject(),
                        c.eventType = a);
                        for (d in b)
                            m(b, d) && (c[d] = b[d]);
                        if (document.createEvent)
                            document.dispatchEvent(c);
                        else
                            try {
                                document.fireEvent("on" + c.eventType.toLowerCase(), c)
                            } catch (e) {}
                    }
                },
                y: function() {
                    function a(a, b, d, e) {
                        var f = a[b];
                        a[b] = d(f),
                        e || c.q.push([a, b, f])
                    }
                    function b(a) {
                        return function(b, d) {
                            var e = [].slice.call(arguments)
                              , f = e[0];
                            return i(f) && (e[0] = c.wrap(f)),
                            a.apply ? a.apply(this, e) : a(e[0], e[1])
                        }
                    }
                    var c = this;
                    a(window, "setTimeout", b),
                    a(window, "setInterval", b),
                    window.requestAnimationFrame && a(window, "requestAnimationFrame", function(a) {
                        return function(b) {
                            return a(c.wrap(b))
                        }
                    }),
                    "EventTarget Window Node ApplicationCache AudioTrackList ChannelMergerNode CryptoOperation EventSource FileReader HTMLUnknownElement IDBDatabase IDBRequest IDBTransaction KeyOperation MediaController MessagePort ModalWindow Notification SVGElementInstance Screen TextTrack TextTrackCue TextTrackList WebSocket WebSocketWorker Worker XMLHttpRequest XMLHttpRequestEventTarget XMLHttpRequestUpload".replace(/\w+/g, function(b) {
                        var d = window[b] && window[b].prototype;
                        d && d.hasOwnProperty && d.hasOwnProperty("addEventListener") && (a(d, "addEventListener", function(a) {
                            return function(b, d, e, f) {
                                try {
                                    d && d.handleEvent && (d.handleEvent = c.wrap(d.handleEvent))
                                } catch (g) {}
                                return a.call(this, b, c.wrap(d), e, f)
                            }
                        }),
                        a(d, "removeEventListener", function(a) {
                            return function(b, c, d, e) {
                                return c = c && (c.C ? c.C : c),
                                a.call(this, b, c, d, e)
                            }
                        }))
                    }),
                    "XMLHttpRequest" in window && a(XMLHttpRequest.prototype, "send", function(b) {
                        return function(d) {
                            var e = this;
                            return "onreadystatechange onload onerror onprogress".replace(/\w+/g, function(b) {
                                b in e && "[object Function]" === Object.prototype.toString.call(e[b]) && a(e, b, function(a) {
                                    return c.wrap(a)
                                }, !0)
                            }),
                            b.apply(this, arguments)
                        }
                    });
                    var d = window.jQuery || window.$;
                    d && d.fn && d.fn.ready && a(d.fn, "ready", function(a) {
                        return function(b) {
                            return a.call(this, c.wrap(b))
                        }
                    })
                },
                E: function() {
                    for (var a; this.q.length; ) {
                        a = this.q.shift();
                        var b = a[0]
                          , c = a[1]
                          , d = a[2];
                        b[c] = d
                    }
                },
                z: function() {
                    var a = this;
                    o(this.o, function(b, c) {
                        var d = c[0]
                          , e = c[1];
                        d.apply(a, [a].concat(e))
                    })
                },
                s: function(a) {
                    var b = u.exec(a)
                      , c = {}
                      , d = 7;
                    try {
                        for (; d--; )
                            c[t[d]] = b[d] || ""
                    } catch (e) {
                        throw new g("Invalid DSN: " + a)
                    }
                    if (c.pass && !this.i.allowSecretKey)
                        throw new g("Do not specify your secret key in the DSN. See: http://bit.ly/raven-secret-key");
                    return c
                },
                v: function(a) {
                    var b = "//" + a.host + (a.port ? ":" + a.port : "");
                    return a.protocol && (b = a.protocol + ":" + b),
                    b
                },
                x: function() {
                    this.j || this.F.apply(this, arguments)
                },
                F: function(a, b) {
                    var c = this
                      , d = [];
                    a.stack && a.stack.length && o(a.stack, function(a, b) {
                        var e = c.J(b);
                        e && d.push(e)
                    }),
                    this.I("handle", {
                        stackInfo: a,
                        options: b
                    }),
                    this.K(a.name, a.message, a.url, a.lineno, d.slice(0, this.i.stackTraceLimit), b)
                },
                J: function(a) {
                    if (a.url) {
                        var b, c = {
                            filename: a.url,
                            lineno: a.line,
                            colno: a.column,
                            "function": a.func || "?"
                        }, d = this.L(a);
                        if (d) {
                            var e = ["pre_context", "context_line", "post_context"];
                            for (b = 3; b--; )
                                c[e[b]] = d[b]
                        }
                        return c.in_app = !(this.i.includePaths.test && !this.i.includePaths.test(c.filename) || /(Raven|TraceKit)\./.test(c["function"]) || /raven\.(min\.)?js$/.test(c.filename)),
                        c
                    }
                },
                L: function(a) {
                    if (a.context && this.i.fetchContext) {
                        for (var b = a.context, c = ~~(b.length / 2), d = b.length, e = !1; d--; )
                            if (b[d].length > 300) {
                                e = !0;
                                break
                            }
                        if (e) {
                            if (j(a.column))
                                return;
                            return [[], b[c].substr(a.column, 50), []]
                        }
                        return [b.slice(0, c), b[c], b.slice(c + 1)]
                    }
                },
                K: function(a, b, c, d, e, f) {
                    var g, h;
                    if ((!this.i.ignoreErrors.test || !this.i.ignoreErrors.test(b)) && (b += "",
                    b = q(b, this.i.maxMessageLength),
                    h = (a ? a + ": " : "") + b,
                    h = q(h, this.i.maxMessageLength),
                    e && e.length ? (c = e[0].filename || c,
                    e.reverse(),
                    g = {
                        frames: e
                    }) : c && (g = {
                        frames: [{
                            filename: c,
                            lineno: d,
                            in_app: !0
                        }]
                    }),
                    (!this.i.ignoreUrls.test || !this.i.ignoreUrls.test(c)) && (!this.i.whitelistUrls.test || this.i.whitelistUrls.test(c)))) {
                        var i = p({
                            exception: {
                                values: [{
                                    type: a,
                                    value: b,
                                    stacktrace: g
                                }]
                            },
                            culprit: c,
                            message: h
                        }, f);
                        this.G(i)
                    }
                },
                M: function(a) {
                    var b = this.i.maxMessageLength;
                    if (a.message = q(a.message, b),
                    a.exception) {
                        var c = a.exception.values[0];
                        c.value = q(c.value, b)
                    }
                    return a
                },
                N: function() {
                    if (this.b && document.location && document.location.href) {
                        var a = {
                            headers: {
                                "User-Agent": navigator.userAgent
                            }
                        };
                        return a.url = document.location.href,
                        document.referrer && (a.headers.Referer = document.referrer),
                        a
                    }
                },
                G: function(a) {
                    var b = this
                      , c = this.i
                      , e = {
                        project: this.g,
                        logger: c.logger,
                        platform: "javascript"
                    }
                      , f = this.N();
                    if (f && (e.request = f),
                    a = p(e, a),
                    a.tags = p(p({}, this.h.tags), a.tags),
                    a.extra = p(p({}, this.h.extra), a.extra),
                    a.extra["session:duration"] = d() - this.p,
                    l(a.tags) && delete a.tags,
                    this.h.user && (a.user = this.h.user),
                    c.release && (a.release = c.release),
                    c.serverName && (a.server_name = c.serverName),
                    i(c.dataCallback) && (a = c.dataCallback(a) || a),
                    a && !l(a) && (!i(c.shouldSendCallback) || c.shouldSendCallback(a)) && (this.d = a.event_id || (a.event_id = s()),
                    a = this.M(a),
                    this.r("debug", "Raven about to send:", a),
                    this.isSetup())) {
                        var g = {
                            sentry_version: "7",
                            sentry_client: "raven-js/" + this.VERSION,
                            sentry_key: this.f
                        };
                        this.u && (g.sentry_secret = this.u);
                        var h = this.w;
                        (c.transport || this.O).call(this, {
                            url: h,
                            auth: g,
                            data: a,
                            options: c,
                            onSuccess: function() {
                                b.I("success", {
                                    data: a,
                                    src: h
                                })
                            },
                            onError: function() {
                                b.I("failure", {
                                    data: a,
                                    src: h
                                })
                            }
                        })
                    }
                },
                P: function(a) {
                    a.auth.sentry_data = JSON.stringify(a.data);
                    var b = this.Q()
                      , c = a.url + "?" + r(a.auth)
                      , d = a.options.crossOrigin;
                    (d || "" === d) && (b.crossOrigin = d),
                    b.onload = a.onSuccess,
                    b.onerror = b.onabort = a.onError,
                    b.src = c
                },
                R: function(a) {
                    function b() {
                        200 === c.status ? a.onSuccess && a.onSuccess() : a.onError && a.onError()
                    }
                    var c, d = a.url;
                    c = new XMLHttpRequest,
                    "withCredentials" in c ? c.onreadystatechange = function() {
                        4 === c.readyState && b()
                    }
                     : (c = new XDomainRequest,
                    d = d.replace(/^https?:/, ""),
                    c.onload = b),
                    c.open("POST", d + "?" + r(a.auth)),
                    c.send(JSON.stringify(a.data))
                },
                O: function(a) {
                    var b = "withCredentials" in new XMLHttpRequest || "undefined" != typeof XDomainRequest;
                    return (b ? this.R : this.P)(a)
                },
                Q: function() {
                    return document.createElement("img")
                },
                r: function(a) {
                    this.n[a] && this.debug && Function.prototype.apply.call(this.n[a], this.m, [].slice.call(arguments, 1))
                },
                H: function(a, b) {
                    j(b) ? delete this.h[a] : this.h[a] = p(this.h[a] || {}, b)
                }
            },
            e.prototype.setUser = e.prototype.setUserContext,
            e.prototype.setReleaseContext = e.prototype.setRelease,
            b.exports = e
        }
        , {
            1: 1,
            4: 4,
            5: 5
        }],
        3: [function(a, b, c) {
            "use strict";
            var d = a(2)
              , e = window.Raven
              , f = new d;
            f.noConflict = function() {
                return window.Raven = e,
                f
            }
            ,
            f.afterLoad(),
            b.exports = f
        }
        , {
            2: 2
        }],
        4: [function(a, b, c) {
            "use strict";
            function d(a) {
                return void 0 === a
            }
            function e(a) {
                return "function" == typeof a
            }
            function f(a) {
                return "[object String]" === q.toString.call(a)
            }
            function g(a) {
                return "object" == typeof a && null  !== a
            }
            function h(a) {
                for (var b in a)
                    return !1;
                return !0
            }
            function i(a) {
                var b = q.toString.call(a);
                return g(a) && "[object Error]" === b || "[object Exception]" === b || a instanceof Error
            }
            function j(a, b) {
                var c, e;
                if (d(a.length))
                    for (c in a)
                        m(a, c) && b.call(null , c, a[c]);
                else if (e = a.length)
                    for (c = 0; e > c; c++)
                        b.call(null , c, a[c])
            }
            function k(a, b) {
                return b ? (j(b, function(b, c) {
                    a[b] = c
                }),
                a) : a
            }
            function l(a, b) {
                return !b || b >= a.length ? a : a.substr(0, b) + "…"
            }
            function m(a, b) {
                return q.hasOwnProperty.call(a, b)
            }
            function n(a) {
                for (var b, c = [], d = 0, e = a.length; e > d; d++)
                    b = a[d],
                    f(b) ? c.push(b.replace(/([.*+?^=!:${}()|\[\]\/\\])/g, "\\$1")) : b && b.source && c.push(b.source);
                return new RegExp(c.join("|"),"i")
            }
            function o(a) {
                var b = [];
                return j(a, function(a, c) {
                    b.push(encodeURIComponent(a) + "=" + encodeURIComponent(c))
                }),
                b.join("&")
            }
            function p() {
                var a = window.crypto || window.msCrypto;
                if (!d(a) && a.getRandomValues) {
                    var b = new Uint16Array(8);
                    a.getRandomValues(b),
                    b[3] = 4095 & b[3] | 16384,
                    b[4] = 16383 & b[4] | 32768;
                    var c = function(a) {
                        for (var b = a.toString(16); 4 > b.length; )
                            b = "0" + b;
                        return b
                    }
                    ;
                    return c(b[0]) + c(b[1]) + c(b[2]) + c(b[3]) + c(b[4]) + c(b[5]) + c(b[6]) + c(b[7])
                }
                return "xxxxxxxxxxxx4xxxyxxxxxxxxxxxxxxx".replace(/[xy]/g, function(a) {
                    var b = 16 * Math.random() | 0
                      , c = "x" === a ? b : 3 & b | 8;
                    return c.toString(16)
                })
            }
            var q = Object.prototype;
            b.exports = {
                isUndefined: d,
                isFunction: e,
                isString: f,
                isObject: g,
                isEmptyObject: h,
                isError: i,
                each: j,
                objectMerge: k,
                truncate: l,
                hasKey: m,
                joinRegExp: n,
                urlencode: o,
                uuid4: p
            }
        }
        , {}],
        5: [function(a, b, c) {
            "use strict";
            function d() {
                return "undefined" == typeof document ? "" : document.location.href
            }
            var e = a(4)
              , f = e.hasKey
              , g = e.isString
              , h = e.isUndefined
              , i = {
                remoteFetching: !1,
                collectWindowErrors: !0,
                linesOfContext: 7,
                debug: !1
            }
              , j = [].slice
              , k = "?"
              , l = /^(?:Uncaught )?((?:Eval|Internal|Range|Reference|Syntax|Type|URI)Error)\: ?(.*)$/;
            i.report = function() {
                function a(a) {
                    k(),
                    r.push(a)
                }
                function b(a) {
                    for (var b = r.length - 1; b >= 0; --b)
                        r[b] === a && r.splice(b, 1)
                }
                function c() {
                    m(),
                    r = []
                }
                function e(a, b) {
                    var c = null ;
                    if (!b || i.collectWindowErrors) {
                        for (var d in r)
                            if (f(r, d))
                                try {
                                    r[d].apply(null , [a].concat(j.call(arguments, 2)))
                                } catch (e) {
                                    c = e
                                }
                        if (c)
                            throw c
                    }
                }
                function h(a, b, c, f, h) {
                    var j = null ;
                    if (u)
                        i.computeStackTrace.augmentStackTraceWithInitialElement(u, b, c, a),
                        n();
                    else if (h)
                        j = i.computeStackTrace(h),
                        e(j, !0);
                    else {
                        var k = {
                            url: b,
                            line: c,
                            column: f
                        };
                        k.func = i.computeStackTrace.guessFunctionName(k.url, k.line),
                        k.context = i.computeStackTrace.gatherContext(k.url, k.line);
                        var m, o = void 0, q = a;
                        if (g(a)) {
                            var m = a.match(l);
                            m && (o = m[1],
                            q = m[2])
                        }
                        j = {
                            name: o,
                            message: q,
                            url: d(),
                            stack: [k]
                        },
                        e(j, !0)
                    }
                    return p ? p.apply(this, arguments) : !1
                }
                function k() {
                    q || (p = window.onerror,
                    window.onerror = h,
                    q = !0)
                }
                function m() {
                    q && (window.onerror = p,
                    q = !1,
                    p = void 0)
                }
                function n() {
                    var a = u
                      , b = s;
                    s = null ,
                    u = null ,
                    t = null ,
                    e.apply(null , [a, !1].concat(b))
                }
                function o(a, b) {
                    var c = j.call(arguments, 1);
                    if (u) {
                        if (t === a)
                            return;
                        n()
                    }
                    var d = i.computeStackTrace(a);
                    if (u = d,
                    t = a,
                    s = c,
                    window.setTimeout(function() {
                        t === a && n()
                    }, d.incomplete ? 2e3 : 0),
                    b !== !1)
                        throw a
                }
                var p, q, r = [], s = null , t = null , u = null ;
                return o.subscribe = a,
                o.unsubscribe = b,
                o.uninstall = c,
                o
            }(),
            i.computeStackTrace = function() {
                function a(a) {
                    if (!i.remoteFetching)
                        return "";
                    try {
                        var b = function() {
                            try {
                                return new window.XMLHttpRequest
                            } catch (a) {
                                return new window.ActiveXObject("Microsoft.XMLHTTP")
                            }
                        }
                          , c = b();
                        return c.open("GET", a, !1),
                        c.send(""),
                        c.responseText
                    } catch (d) {
                        return ""
                    }
                }
                function b(b) {
                    if (!g(b))
                        return [];
                    if (!f(v, b)) {
                        var c = ""
                          , d = "";
                        try {
                            d = document.domain
                        } catch (e) {}
                        -1 !== b.indexOf(d) && (c = a(b)),
                        v[b] = c ? c.split("\n") : []
                    }
                    return v[b]
                }
                function c(a, c) {
                    var d, e = /function ([^(]*)\(([^)]*)\)/, f = /['"]?([0-9A-Za-z$_]+)['"]?\s*[:=]\s*(function|eval|new Function)/, g = "", i = 10, j = b(a);
                    if (!j.length)
                        return k;
                    for (var l = 0; i > l; ++l)
                        if (g = j[c - l] + g,
                        !h(g)) {
                            if (d = f.exec(g))
                                return d[1];
                            if (d = e.exec(g))
                                return d[1]
                        }
                    return k
                }
                function e(a, c) {
                    var d = b(a);
                    if (!d.length)
                        return null ;
                    var e = []
                      , f = Math.floor(i.linesOfContext / 2)
                      , g = f + i.linesOfContext % 2
                      , j = Math.max(0, c - f - 1)
                      , k = Math.min(d.length, c + g - 1);
                    c -= 1;
                    for (var l = j; k > l; ++l)
                        h(d[l]) || e.push(d[l]);
                    return e.length > 0 ? e : null 
                }
                function j(a) {
                    return a.replace(/[\-\[\]{}()*+?.,\\\^$|#]/g, "\\$&")
                }
                function l(a) {
                    return j(a).replace("<", "(?:<|&lt;)").replace(">", "(?:>|&gt;)").replace("&", "(?:&|&amp;)").replace('"', '(?:"|&quot;)').replace(/\s+/g, "\\s+")
                }
                function m(a, c) {
                    for (var d, e, f = 0, g = c.length; g > f; ++f)
                        if ((d = b(c[f])).length && (d = d.join("\n"),
                        e = a.exec(d)))
                            return {
                                url: c[f],
                                line: d.substring(0, e.index).split("\n").length,
                                column: e.index - d.lastIndexOf("\n", e.index) - 1
                            };
                    return null 
                }
                function n(a, c, d) {
                    var e, f = b(c), g = new RegExp("\\b" + j(a) + "\\b");
                    return d -= 1,
                    f && f.length > d && (e = g.exec(f[d])) ? e.index : null 
                }
                function o(a) {
                    if ("undefined" != typeof document) {
                        for (var b, c, d, e, f = [window.location.href], g = document.getElementsByTagName("script"), h = "" + a, i = /^function(?:\s+([\w$]+))?\s*\(([\w\s,]*)\)\s*\{\s*(\S[\s\S]*\S)\s*\}\s*$/, k = /^function on([\w$]+)\s*\(event\)\s*\{\s*(\S[\s\S]*\S)\s*\}\s*$/, n = 0; g.length > n; ++n) {
                            var o = g[n];
                            o.src && f.push(o.src)
                        }
                        if (d = i.exec(h)) {
                            var p = d[1] ? "\\s+" + d[1] : ""
                              , q = d[2].split(",").join("\\s*,\\s*");
                            b = j(d[3]).replace(/;$/, ";?"),
                            c = new RegExp("function" + p + "\\s*\\(\\s*" + q + "\\s*\\)\\s*{\\s*" + b + "\\s*}")
                        } else
                            c = new RegExp(j(h).replace(/\s+/g, "\\s+"));
                        if (e = m(c, f))
                            return e;
                        if (d = k.exec(h)) {
                            var r = d[1];
                            if (b = l(d[2]),
                            c = new RegExp("on" + r + "=[\\'\"]\\s*" + b + "\\s*[\\'\"]","i"),
                            e = m(c, f[0]))
                                return e;
                            if (c = new RegExp(b),
                            e = m(c, f))
                                return e
                        }
                        return null 
                    }
                }
                function p(a) {
                    if (!h(a.stack) && a.stack) {
                        for (var b, f, g = /^\s*at (.*?) ?\(((?:file|https?|blob|chrome-extension|native|eval|<anonymous>).*?)(?::(\d+))?(?::(\d+))?\)?\s*$/i, i = /^\s*(.*?)(?:\((.*?)\))?(?:^|@)((?:file|https?|blob|chrome|\[native).*?)(?::(\d+))?(?::(\d+))?\s*$/i, j = /^\s*at (?:((?:\[object object\])?.+) )?\(?((?:ms-appx|https?|blob):.*?):(\d+)(?::(\d+))?\)?\s*$/i, l = a.stack.split("\n"), m = [], o = /^(.*) is undefined$/.exec(a.message), p = 0, q = l.length; q > p; ++p) {
                            if (b = g.exec(l[p])) {
                                var r = b[2] && -1 !== b[2].indexOf("native");
                                f = {
                                    url: r ? null  : b[2],
                                    func: b[1] || k,
                                    args: r ? [b[2]] : [],
                                    line: b[3] ? +b[3] : null ,
                                    column: b[4] ? +b[4] : null 
                                }
                            } else if (b = j.exec(l[p]))
                                f = {
                                    url: b[2],
                                    func: b[1] || k,
                                    args: [],
                                    line: +b[3],
                                    column: b[4] ? +b[4] : null 
                                };
                            else {
                                if (!(b = i.exec(l[p])))
                                    continue;f = {
                                    url: b[3],
                                    func: b[1] || k,
                                    args: b[2] ? b[2].split(",") : [],
                                    line: b[4] ? +b[4] : null ,
                                    column: b[5] ? +b[5] : null 
                                }
                            }
                            !f.func && f.line && (f.func = c(f.url, f.line)),
                            f.line && (f.context = e(f.url, f.line)),
                            m.push(f)
                        }
                        return m.length ? (m[0].line && !m[0].column && o ? m[0].column = n(o[1], m[0].url, m[0].line) : m[0].column || h(a.columnNumber) || (m[0].column = a.columnNumber + 1),
                        {
                            name: a.name,
                            message: a.message,
                            url: d(),
                            stack: m
                        }) : null 
                    }
                }
                function q(a) {
                    var b = a.stacktrace;
                    if (!h(a.stacktrace) && a.stacktrace) {
                        for (var f, g = / line (\d+).*script (?:in )?(\S+)(?:: in function (\S+))?$/i, i = / line (\d+), column (\d+)\s*(?:in (?:<anonymous function: ([^>]+)>|([^\)]+))\((.*)\))? in (.*):\s*$/i, j = b.split("\n"), k = [], l = 0; j.length > l; l += 2) {
                            var m = null ;
                            if ((f = g.exec(j[l])) ? m = {
                                url: f[2],
                                line: +f[1],
                                column: null ,
                                func: f[3],
                                args: []
                            } : (f = i.exec(j[l])) && (m = {
                                url: f[6],
                                line: +f[1],
                                column: +f[2],
                                func: f[3] || f[4],
                                args: f[5] ? f[5].split(",") : []
                            }),
                            m) {
                                if (!m.func && m.line && (m.func = c(m.url, m.line)),
                                m.line)
                                    try {
                                        m.context = e(m.url, m.line)
                                    } catch (n) {}
                                m.context || (m.context = [j[l + 1]]),
                                k.push(m)
                            }
                        }
                        return k.length ? {
                            name: a.name,
                            message: a.message,
                            url: d(),
                            stack: k
                        } : null 
                    }
                }
                function r(a) {
                    var g = a.message.split("\n");
                    if (4 > g.length)
                        return null ;
                    var h, i = /^\s*Line (\d+) of linked script ((?:file|https?|blob)\S+)(?:: in function (\S+))?\s*$/i, j = /^\s*Line (\d+) of inline#(\d+) script in ((?:file|https?|blob)\S+)(?:: in function (\S+))?\s*$/i, k = /^\s*Line (\d+) of function script\s*$/i, n = [], o = document.getElementsByTagName("script"), p = [];
                    for (var q in o)
                        f(o, q) && !o[q].src && p.push(o[q]);
                    for (var r = 2; g.length > r; r += 2) {
                        var s = null ;
                        if (h = i.exec(g[r]))
                            s = {
                                url: h[2],
                                func: h[3],
                                args: [],
                                line: +h[1],
                                column: null 
                            };
                        else if (h = j.exec(g[r])) {
                            s = {
                                url: h[3],
                                func: h[4],
                                args: [],
                                line: +h[1],
                                column: null 
                            };
                            var t = +h[1]
                              , u = p[h[2] - 1];
                            if (u) {
                                var v = b(s.url);
                                if (v) {
                                    v = v.join("\n");
                                    var w = v.indexOf(u.innerText);
                                    w >= 0 && (s.line = t + v.substring(0, w).split("\n").length)
                                }
                            }
                        } else if (h = k.exec(g[r])) {
                            var x = window.location.href.replace(/#.*$/, "")
                              , y = new RegExp(l(g[r + 1]))
                              , z = m(y, [x]);
                            s = {
                                url: x,
                                func: "",
                                args: [],
                                line: z ? z.line : h[1],
                                column: null 
                            }
                        }
                        if (s) {
                            s.func || (s.func = c(s.url, s.line));
                            var A = e(s.url, s.line)
                              , B = A ? A[Math.floor(A.length / 2)] : null ;
                            s.context = A && B.replace(/^\s*/, "") === g[r + 1].replace(/^\s*/, "") ? A : [g[r + 1]],
                            n.push(s)
                        }
                    }
                    return n.length ? {
                        name: a.name,
                        message: g[0],
                        url: d(),
                        stack: n
                    } : null 
                }
                function s(a, b, d, f) {
                    var g = {
                        url: b,
                        line: d
                    };
                    if (g.url && g.line) {
                        a.incomplete = !1,
                        g.func || (g.func = c(g.url, g.line)),
                        g.context || (g.context = e(g.url, g.line));
                        var h = / '([^']+)' /.exec(f);
                        if (h && (g.column = n(h[1], g.url, g.line)),
                        a.stack.length > 0 && a.stack[0].url === g.url) {
                            if (a.stack[0].line === g.line)
                                return !1;
                            if (!a.stack[0].line && a.stack[0].func === g.func)
                                return a.stack[0].line = g.line,
                                a.stack[0].context = g.context,
                                !1
                        }
                        return a.stack.unshift(g),
                        a.partial = !0,
                        !0
                    }
                    return a.incomplete = !0,
                    !1
                }
                function t(a, b) {
                    for (var e, f, g, h = /function\s+([_$a-zA-Z\xA0-\uFFFF][_$a-zA-Z0-9\xA0-\uFFFF]*)?\s*\(/i, j = [], l = {}, m = !1, p = t.caller; p && !m; p = p.caller)
                        if (p !== u && p !== i.report) {
                            if (f = {
                                url: null ,
                                func: k,
                                line: null ,
                                column: null 
                            },
                            p.name ? f.func = p.name : (e = h.exec(p.toString())) && (f.func = e[1]),
                            "undefined" == typeof f.func)
                                try {
                                    f.func = e.input.substring(0, e.input.indexOf("{"))
                                } catch (q) {}
                            if (g = o(p)) {
                                f.url = g.url,
                                f.line = g.line,
                                f.func === k && (f.func = c(f.url, f.line));
                                var r = / '([^']+)' /.exec(a.message || a.description);
                                r && (f.column = n(r[1], g.url, g.line))
                            }
                            l["" + p] ? m = !0 : l["" + p] = !0,
                            j.push(f)
                        }
                    b && j.splice(0, b);
                    var v = {
                        name: a.name,
                        message: a.message,
                        url: d(),
                        stack: j
                    };
                    return s(v, a.sourceURL || a.fileName, a.line || a.lineNumber, a.message || a.description),
                    v
                }
                function u(a, b) {
                    var c = null ;
                    b = null  == b ? 0 : +b;
                    try {
                        if (c = q(a))
                            return c
                    } catch (e) {
                        if (i.debug)
                            throw e
                    }
                    try {
                        if (c = p(a))
                            return c
                    } catch (e) {
                        if (i.debug)
                            throw e
                    }
                    try {
                        if (c = r(a))
                            return c
                    } catch (e) {
                        if (i.debug)
                            throw e
                    }
                    try {
                        if (c = t(a, b + 1))
                            return c
                    } catch (e) {
                        if (i.debug)
                            throw e
                    }
                    return {
                        name: a.name,
                        message: a.message,
                        url: d()
                    }
                }
                var v = {};
                return u.augmentStackTraceWithInitialElement = s,
                u.computeStackTraceFromStackProp = p,
                u.guessFunctionName = c,
                u.gatherContext = e,
                u
            }(),
            b.exports = i
        }
        , {
            4: 4
        }]
    }, {}, [3])(3)
}),
!function() {
    "undefined" != typeof Raven && "undefined" != typeof SentryDsn && (Raven.config(SentryDsn).install(),
    "undefined" != typeof Crew && "undefined" != typeof Crew.User && Raven.setUserContext(Crew.User))
}();
var Crew = Crew || {};
!function(a, b, c) {
    "use strict";
    var d = b("body")
      , e = function() {
        var a = d.css("content");
        return a ? a.replace(/"/g, "") : !1
    }
      , f = function() {
        Crew.Mq = g(e()),
        a.addEventListener("resize", _.throttle(function() {
            Crew.Mq = g(e())
        }, 300))
    }
      , g = function(a) {
        return {
            xs: "xs" === a,
            sm: "sm" === a,
            md: "md" === a,
            lg: "lg" === a
        }
    }
    ;
    b(function() {
        f()
    })
}(window, jQuery);
var Crew = Crew || {};
Crew.Modules = Crew.Modules || {},
function(a, b, c) {
    Crew.Modules.ets = function(b) {
        var c, d, e, f, g, h;
        return c = d = {},
        e = {},
        f = {},
        g = {},
        h = {
            LS_ID: "crew.ets",
            TOGGLE_SELECTOR: ".js-enter-to-submit-toggle",
            BUTTON_SELECTOR: ".js-enter-to-submit-button",
            TEXTAREA_SELECTOR: ".js-enter-to-submit-textarea",
            AUTOSIZE_CLASS: "js-auto-size"
        },
        c.init = function() {
            this.$form = b,
            this.$toggle = this.$form.find(h.TOGGLE_SELECTOR),
            this.$button = this.$form.find(h.BUTTON_SELECTOR),
            this.$textarea = this.$form.find(h.TEXTAREA_SELECTOR)
        }
        ,
        e.init = function() {
            this.enabled = a.localStorage.getItem(h.LS_ID),
            null  === this.enabled ? Crew.Mq.xs ? this.setEnabled(!1) : this.setEnabled(!0) : this.enabled = "true" === this.enabled
        }
        ,
        e.setEnabled = function(b) {
            this.enabled = b,
            a.localStorage.setItem(h.LS_ID, this.enabled)
        }
        ,
        f.init = function() {
            d.$toggle.prop("checked", e.enabled)
        }
        ,
        g.init = function() {
            this.listenForEnabledToggle(),
            this.listenForKeyEvents()
        }
        ,
        g.listenForEnabledToggle = function() {
            d.$toggle.on("change", function() {
                e.setEnabled(this.checked)
            })
        }
        ,
        g.listenForKeyEvents = function() {
            d.$textarea.on("keydown", function(a) {
                e.enabled && (13 !== a.keyCode || a.shiftKey || a.altKey || a.preventDefault())
            }),
            d.$textarea.on("keyup", function(a) {
                var b, c;
                e.enabled && 13 === a.keyCode && (a.shiftKey || a.altKey ? (b = this.value,
                c = Crew.Utils.DOM.getCaretPosition(this),
                this.value = b.substring(0, c - 1) + "\n" + b.substring(c, b.length),
                a.stopPropagation()) : g.submit())
            }),
            d.$button.on("click", g.submit)
        }
        ,
        g.submit = function() {
            d.$textarea.val();
            "" !== d.$textarea.val().trim() ? (d.$form.trigger("submit"),
            d.$textarea[0].value = "",
            d.$textarea.hasClass(h.AUTOSIZE_CLASS) && autosize.update(d.$textarea[0])) : d.$textarea.fadeTo(100, .5).fadeTo(100, 1).fadeTo(100, .5).fadeTo(100, 1)
        }
        ,
        g.teardown = function() {}
        ,
        c.init(),
        e.init(),
        f.init(),
        g.init(),
        {
            teardown: g.teardown
        }
    }
}(window, jQuery);
var Crew = Crew || {};
Crew.Utils = Crew.Utils || {},
Crew.Utils.DOM = Crew.Utils.DOM || {},
Crew.Utils.DOM.getCaretPosition = function(a) {
    if (a.selectionStart)
        return a.selectionStart;
    if (document.selection) {
        a.focus();
        var b = document.selection.createRange();
        if (null  === b)
            return 0;
        var c = a.createTextRange()
          , d = c.duplicate();
        return c.moveToBookmark(b.getBookmark()),
        d.setEndPoint("EndToStart", c),
        d.text.length
    }
    return 0
}
;
var Crew = Crew || {};
Crew.Utils = Crew.Utils || {},
Crew.Utils.eventify = function(a) {
    var b = {}
      , c = -1;
    return a.subscribe = function(a, d) {
        var e = (++c).toString();
        return b[a] || (b[a] = []),
        b[a].push({
            token: e,
            func: d
        }),
        e
    }
    ,
    a.publish = function(a, c) {
        return b[a] ? (setTimeout(function() {
            for (var d = b[a], e = d ? d.length : 0; e--; )
                d[e].func(c)
        }, 0),
        !0) : !1
    }
    ,
    a.unsubscribe = function(a) {
        for (var c in b)
            if (b[c])
                for (var d = 0, e = b[c].length; e > d; d++)
                    if (b[c][d].token === a)
                        return b[c].splice(d, 1),
                        a;
        return !1
    }
    ,
    a.unsubscribeAll = function() {
        return b = {},
        !1
    }
    ,
    a
}
;
var Crew = Crew || {};
Crew.Utils = Crew.Utils || {},
function(a, b, c) {
    var d = function() {
        var a = []
          , b = function(b) {
            if ("function" != typeof b)
                throw new Error("`callback` must be a function.");
            a.push(b)
        }
          , c = function(b, c) {
            _.each(a, function(a) {
                a(b, c)
            })
        }
        ;
        return {
            add: b,
            runAll: c
        }
    }
    ;
    b.extend(Crew.Utils, {
        initializers: d(),
        terminators: d()
    })
}(window, jQuery);
var Crew = Crew || {};
Crew.Utils = Crew.Utils || {},
Crew.Utils.scroll = function() {
    var a = ".js-scrollable"
      , b = function(b) {
        return b.find(a).first()
    }
      , c = function(a, c) {
        var d = b(a);
        d.length && c(d)
    }
      , d = function(a) {
        c(a, function(a) {
            a[0].scrollTop = 0
        })
    }
      , e = function(a) {
        c(a, function(a) {
            a[0].scrollTop = a[0].scrollHeight
        })
    }
    ;
    return {
        reset: d,
        toBottom: e
    }
}();
var Crew = Crew || {};
Crew.Utils = Crew.Utils || {},
Crew.Utils.html = Crew.Utils.html || {},
Crew.Utils.html.strip_tags = function(a, b) {
    b = (((b || "") + "").toLowerCase().match(/<[a-z][a-z0-9]*>/g) || []).join("");
    var c = /<\/?([a-z][a-z0-9]*)\b[^>]*>/gi
      , d = /<!--[\s\S]*?-->|<\?(?:php)?[\s\S]*?\?>/gi;
    return a.replace(d, "").replace(c, function(a, c) {
        return b.indexOf("<" + c.toLowerCase() + ">") > -1 ? a : ""
    })
}
,
Crew.Utils.html.autop = function(a) {
    return ("<p>" + a.replace(/(\r\n|\r)([ \t]*)(\r\n|\r)+/g, "</p><p>").replace(/\r\n|\r/g, "<br />") + "</p>").replace("<p>\\s*?</p>", "")
}
,
Crew.Utils.html.autolink = function(a, b) {
    var c = ["_self", "_blank", "_parent", "_self"]
      , d = c[0];
    return b = ~c.indexOf(b) ? b : d,
    a.replace(/((http|https|ftp):\/\/[\w?=&.\/-;#~%-]+(?![\w\s?&.\/;#~%"=-]*>))/g, '<a href="$1" target="' + b + '"">$1</a> ')
}
,
Crew.Utils.html.htmlentities = function(a) {
    return String(a).replace(/&/g, "&amp;").replace(/</g, "&lt;").replace(/>/g, "&gt;").replace(/"/g, "&quot;")
}
;
var Crew = Crew || {};
Crew.Utils = Crew.Utils || {},
Crew.Utils.time = Crew.Utils.time || {},
Crew.Utils.time.formatAmPm = function(a) {
    var b = a.getHours()
      , c = a.getMinutes()
      , d = b >= 12 ? "pm" : "am";
    return b %= 12,
    b = b ? b : 12,
    c = 10 > c ? "0" + c : c,
    b + ":" + c + " " + d
}
;
var Crew = Crew || {};
Crew.Utils = Crew.Utils || {},
Crew.Utils.Money = Crew.Utils.Money || {},
Crew.Utils.Money.formatCurrency = function(a) {
    return a.toFixed(2).replace(/(\d)(?=(\d{3})+\.)/g, "$1,")
}
;
var Crew = Crew || {};
Crew.Utils = Crew.Utils || {},
Crew.Utils.moduleManager = function(a, b) {
    var c, d, e;
    c = {},
    d = a.replace(/\./g, ""),
    e = 0;
    var f = function(f) {
        f.find(a).each(function(a) {
            var f = this.id;
            f || (f = d + e,
            e++,
            this.id = f),
            c[f] = b($(this))
        })
    }
      , g = function(b) {
        b.find(a).each(function() {
            var a = this.id;
            c.hasOwnProperty(a) && c[a] && (c[a].teardown(),
            c[a] = null )
        })
    }
    ;
    return {
        create: f,
        remove: g
    }
}
;
var Crew = Crew || {};
Crew.modules = Crew.modules || {},
Crew.modules.regions = Crew.modules.regions || {},
Crew.modules.regions.base = function() {
    var a = null 
      , b = null 
      , c = null 
      , d = []
      , e = []
      , f = !1
      , g = 200
      , h = Crew.Utils.eventify({})
      , i = function(d) {
        var e;
        a = d,
        c = a.data("regionId") || j(a),
        b = a.data("url") || "",
        e = a.data("preRendered"),
        e = "boolean" == typeof e ? e : !0,
        "" === b || e ? window.setTimeout(function() {
            o()
        }, 0) : r()
    }
      , j = function(a) {
        var b = Crew.Layout.getNextRegionId();
        return a.data("region-id", b),
        b
    }
      , k = function(b) {
        b = "pre" === b ? b : "post",
        "post" === b && (a.removeClass("pre-update"),
        a.removeClass("pre-update-active")),
        a.addClass(b + "-update"),
        window.setTimeout(function() {
            a.addClass(b + "-update-active")
        }, 1),
        "post" === b && window.setTimeout(function() {
            a.removeClass("post-update"),
            a.removeClass("post-update-active")
        }, g)
    }
      , l = function() {
        k("pre"),
        Crew.Utils.terminators.runAll(a, c),
        h.publish("willUpdate")
    }
      , m = function() {
        Crew.Utils.initializers.runAll(a, c),
        a.find(".js-filepicker").each(function() {
            filepicker.constructWidget(this)
        })
    }
      , n = function(b) {
        a.html(b),
        Crew.Utils.scroll.reset(a),
        m(),
        o()
    }
      , o = function() {
        k("post"),
        h.publish("show")
    }
      , p = function() {
        console.log("Could not load the requested content."),
        k("post")
    }
      , q = function() {
        var a = !1
          , b = _.some(d, function(a) {
            return !a.isUpdatable()
        });
        return f || b || (a = !0),
        a
    }
      , r = function() {
        var a = $.Deferred();
        return q() ? (f = !0,
        l(),
        window.setTimeout(function() {
            $.ajax({
                url: b,
                type: "GET"
            }).done(function(b) {
                n(b),
                f = !1,
                a.resolve(c)
            }).fail(function(b) {
                f = !1,
                p(),
                n(b.responseText),
                a.reject(c)
            })
        }, 0),
        a) : a.reject(c)
    }
      , s = function(a) {
        d.push(a)
    }
      , t = function(a) {
        e.push(a)
    }
      , u = function() {
        d = []
    }
      , v = function(a) {
        d = _.filter(d, function(b) {
            return b.getId() !== a
        })
    }
      , w = function() {
        return c
    }
      , x = function() {
        return b
    }
      , y = function() {
        return a
    }
      , z = function() {
        return e
    }
      , A = function() {
        return d
    }
      , B = function(a) {
        b = a || b
    }
    ;
    return $.extend(h, {
        init: i,
        update: r,
        addSubRegion: s,
        addParentRegion: t,
        removeSubRegions: u,
        removeSubRegionById: v,
        isUpdatable: q,
        getId: w,
        getUrl: x,
        getElement: y,
        getParentRegions: z,
        getSubRegions: A,
        setUrl: B
    })
}
;
var Crew = Crew || {};
Crew.modules = Crew.modules || {},
Crew.modules.regions = Crew.modules.regions || {},
function(a, b, c) {
    Crew.modules.regions.content = function(a) {
        return a.subscribe("show", function() {
            var b = a.getUrl();
            ~b.indexOf("/matches/ajax_find/") && a.setUrl("/matches/ajax_find")
        }),
        a
    }
}(window, jQuery);
var Crew = Crew || {};
Crew.modules = Crew.modules || {},
Crew.modules.regions = Crew.modules.regions || {},
function(a, b, c) {
    Crew.modules.regions.header = function(a) {
        var b, c = null , d = null , e = null , f = "";
        b = {
            BACK_LINK_CLASS: ".js-project-back-link",
            DISABLED_CLASS: "disabled"
        };
        var g = function() {
            a.resetTitle(),
            d.addClass(b.DISABLED_CLASS)
        }
          , h = function() {
            a.resetTitle(),
            d.removeClass(b.DISABLED_CLASS)
        }
        ;
        return a.updateBackLink = function(a) {
            var b, c = a.urlParams;
            d.length && (b = c.hasOwnProperty("content") && ~c.content.indexOf("projects"),
            !b || b && !c.hasOwnProperty("middle") || b && (~c.middle.indexOf("channels") || ~c.middle.indexOf("ajax_middle_region")) ? g() : h())
        }
        ,
        a.setTitlePrefix = function(a) {
            a = Crew.Utils.html.strip_tags(a),
            e.html(a + f)
        }
        ,
        a.resetTitle = function(a) {
            e.html(f)
        }
        ,
        a.subscribe("show", function() {
            c = a.getElement(),
            d = c.find(b.BACK_LINK_CLASS),
            e = c.find(b.BACK_LINK_CLASS + " h1"),
            f = e.html(),
            a.updateBackLink(Crew.getCurrentState())
        }),
        a
    }
}(window, jQuery);
var Crew = Crew || {};
Crew.modules = Crew.modules || {},
Crew.modules.regions = Crew.modules.regions || {},
function(a, b, c) {
    Crew.modules.regions.middle = function(a) {
        return a.subscribe("willUpdate", function() {
            Crew.Mq.xs && Crew.Mobile.resetColumnsVisibility()
        }),
        a
    }
}(window, jQuery);
var Crew = Crew || {};
Crew.modules = Crew.modules || {},
Crew.modules.regions = Crew.modules.regions || {},
function(a, b, c) {
    Crew.modules.regions.projectsList = function(a) {
        var c = null 
          , d = null 
          , e = null 
          , f = null 
          , g = null 
          , h = null 
          , i = function(a) {
            var b;
            return f.hasOwnProperty(a) || (b = c.find('[data-project-id="' + a + '"]'),
            b.length && (f[a] = b)),
            f[a]
        }
          , j = function(a, b) {
            var c = i(a)
              , d = !1;
            h[a] && (d = h[a]["class"]),
            c && c.length && b["class"] !== d && (c.removeClass(Crew.Notifications.classes),
            c.addClass(b["class"]))
        }
          , k = function() {
            return e ? e : Crew.Layout.getRegion("content")
        }
        ;
        return a.subscribe("show", function() {
            c = a.getElement(),
            d = c.find(".js-nav-items"),
            f = {},
            h = {},
            g = c.find("[data-project-last-modified]").first().data("projectLastModified")
        }),
        b(".js-default-content").on("click", ".js-project-link", function() {
            var a, b;
            a = k(),
            a && (b = a.subscribe("willUpdate", function() {
                d.trigger("updateNeeded"),
                a.unsubscribe(b)
            }))
        }),
        Crew.Notifications.subscribe("ping", function(b) {
            b.hasOwnProperty("projects") && b.projects.hasOwnProperty("meta") && (b.projects.meta.projectLastModified && g < b.projects.meta.projectLastModified ? a.update() : (Object.keys(b.projects.status).forEach(function(a) {
                j(a, b.projects.status[a])
            }),
            h = b.projects.status))
        }),
        a
    }
}(window, jQuery);
var Crew = Crew || {};
Crew.modules = Crew.modules || {},
Crew.modules.regions = Crew.modules.regions || {},
function(a, b, c) {
    Crew.modules.regions.segmentsList = function(a) {
        var b = null 
          , c = null 
          , d = null ;
        a.subscribe("show", function() {
            b = a.getElement(),
            c = b.find("[data-project-id]").first().data("projectId"),
            d = b.find("[data-last-modified-segment]").first().data("lastModifiedSegment")
        });
        var e = Crew.Notifications.subscribe("ping", function(b) {
            var f, g;
            b.hasOwnProperty("projects") && b.projects.hasOwnProperty("status") && b.projects.status.hasOwnProperty(c) && (f = b.projects.status[c],
            f.lastModifiedSegment && d < f.lastModifiedSegment && (g = a.getParentRegions()[0],
            "right" === g.getId() ? (Crew.Notifications.unsubscribe(e),
            g.update()) : a.update()))
        });
        return a
    }
}(window, jQuery);
var Crew = Crew || {};
Crew.modules = Crew.modules || {},
Crew.modules.layout = function() {
    var a = {}
      , b = null 
      , c = 0
      , d = Crew.Utils.eventify({})
      , e = function(a, c) {
        b = c,
        $(a).closestDescendant(b, !0).each(function() {
            j(f($(this)))
        })
    }
      , f = function(a, b) {
        var c, e = Crew.modules.regions.base();
        return e.init(a),
        c = e.getId(),
        Crew.modules.regions.hasOwnProperty(c) && (e = Crew.modules.regions[c](e)),
        "undefined" != typeof b && (b.addSubRegion(e),
        e.addParentRegion(b)),
        d.publish("regionInitialized", e),
        e
    }
      , g = function(b) {
        return a[b] ? (a[b].unsubscribeAll(),
        a[b] = null ,
        !0) : !1
    }
      , h = function(a) {
        return _.each(a, function(a) {
            var b = a.getId()
              , c = a.getSubRegions();
            g(b),
            c.length && h(c)
        }),
        !0
    }
      , i = function(c) {
        var d = a[c];
        d.subscribe("willUpdate", function() {
            h(d.getSubRegions()),
            d.removeSubRegions()
        }),
        d.subscribe("show", function() {
            var a = d.getElement().closestDescendant(b, !0);
            a.length && a.each(function() {
                j(f($(this), d))
            })
        })
    }
      , j = function(b) {
        var c = b.getId();
        if (a[c])
            throw new Error("Region '" + c + "' already exists.");
        return a[c] = b,
        i(c),
        a[c]
    }
      , k = function(b) {
        return a[b] ? a[b] : !1
    }
      , l = function() {
        return a
    }
      , m = function() {
        return c += 1,
        "region-" + c
    }
    ;
    return $.extend(d, {
        init: e,
        addRegion: j,
        getRegion: k,
        getRegions: l,
        getNextRegionId: m
    })
}
;
var Crew = Crew || {};
!function(a, b, c) {
    var d, e, f, g, h, i;
    e = d = {},
    f = {},
    h = {},
    g = {},
    i = {
        CONTAINER: ".js-default-content",
        LAYOUT_REGIONS_SELECTOR: ".js-region",
        REGION_RELOAD_TRIGGER_SELECTOR: "[data-reload-regions]"
    },
    e.init = function() {
        this.$container = b(i.CONTAINER)
    }
    ,
    f.init = function() {
        this.baseUrl = [a.location.protocol, "//", a.location.host, "/app"].join(""),
        this.urlParams = h.getCurrentUrlParams(),
        a.history.replaceState(this.urlParams, "", a.location.href)
    }
    ,
    f.getCurrentState = function() {
        return f
    }
    ,
    h.init = function() {
        Crew.Layout = Crew.modules.layout(),
        Crew.Layout.init(i.CONTAINER, i.LAYOUT_REGIONS_SELECTOR)
    }
    ,
    h.getSelfId = function(a) {
        return a.closest(i.LAYOUT_REGIONS_SELECTOR).data("regionId")
    }
    ,
    h.getCurrentUrlParams = function() {
        return b.deparam(decodeURIComponent(a.location.search.substr(1)))
    }
    ,
    g.init = function() {
        this.listenForRegionReloadEvent(),
        this.listenForPopStateEvents()
    }
    ,
    g.listenForPopStateEvents = function() {
        a.onpopstate = function(b) {
            b.state && a.location.reload()
        }
    }
    ,
    g.listenForRegionReloadEvent = function() {
        d.$container.on("click", i.REGION_RELOAD_TRIGGER_SELECTOR, function(a) {
            var c, d;
            a.preventDefault(),
            c = b(this),
            d = c.data("reloadRegions"),
            g.updateUrl(c, d),
            g.triggerRegionsReload(c, d),
            g.updateTitle(c.attr("title"))
        })
    }
    ,
    g.triggerRegionsReload = function(a, c) {
        var d = [];
        Object.keys(c).forEach(function(b) {
            var e;
            if (e = Crew.Layout.getRegion("self" === b ? h.getSelfId(a) : b)) {
                if (("middle" === b || "right" === b) && e.getUrl() === c[b])
                    return;
                null  !== c[b] && e.setUrl(c[b]),
                d.push(e.update())
            }
        }),
        d.length && b.when.apply(b, d).done(function() {
            Crew.Layout.publish("regionUpdated", arguments)
        })
    }
    ,
    g.updateTitle = function(a) {
        a && (document.title = a)
    }
    ,
    g.updateUrl = function(c, d) {
        var e, i = !1, j = ["content", "middle", "right"], k = function(a) {
            _.each(a, function(a) {
                var b = a.getId()
                  , c = a.getSubRegions();
                f.urlParams[b] && delete f.urlParams[b],
                c.length && k(c)
            })
        }
        ;
        Object.keys(d).forEach(function(a) {
            var b, e = "self" === a ? h.getSelfId(c) : a;
            ~j.indexOf(e) && (b = Crew.Layout.getRegion(e),
            b && k(b.getSubRegions()),
            f.urlParams[e] = d[a],
            i = !0)
        }),
        i && (e = decodeURIComponent(b.param(f.urlParams)),
        a.history.pushState(f.urlParams, "", f.baseUrl + "?" + e),
        Object.keys(f.urlParams).length > 1 && g.updateHeader())
    }
    ,
    g.updateHeader = function() {
        var a = Crew.Layout.getRegion("header");
        a && a.updateBackLink(f)
    }
    ,
    b(function() {
        e.init(),
        f.init(),
        h.init(),
        g.init(),
        b.extend(a.Crew, {
            getCurrentState: f.getCurrentState
        })
    })
}(window, jQuery),
function(a, b, c) {
    var d, e, f;
    d = e = {},
    f = {},
    d.init = function() {
        this.$menu = b(".js-open-mobile-menu")
    }
    ,
    f.init = function() {
        e.$menu.length && e.$menu.bigSlide({
            menu: "#mobile-menu",
            side: "left",
            push: ".push-menu"
        })
    }
    ,
    b(function() {
        d.init(),
        f.init()
    })
}(window, jQuery),
function(a, b, c) {
    function d(a) {
        isNaN(a) && (a = 0),
        k.$fee.text(a.toFixed(2))
    }
    function e(a) {
        isNaN(a) && (a = 0),
        k.$final.text(a.toFixed(2))
    }
    function f() {
        setTimeout(function() {
            k.value < k.minimum && !k.isError && g()
        }, 750),
        k.value >= k.minimum && k.isError && h()
    }
    function g() {
        k.$controlGroup.addClass("error").children("label").addClass("text-error"),
        k.isError = !0
    }
    function h() {
        k.$controlGroup.removeClass("error").children("label").removeClass("text-error"),
        k.isError = !1
    }
    function i() {
        var a, b;
        k.value = k.$input.val() || 0,
        b = k.value / (1 - k.fee_percentage),
        a = b * k.fee_percentage,
        e(b),
        d(a),
        f()
    }
    function j() {
        k.$input.on("keyup", function() {
            i()
        })
    }
    var k, l;
    k = l = {
        $input: b(".js-budget-input"),
        $fee: b(".js-budget-fee"),
        $final: b(".js-budget-final"),
        $controlGroup: b(".js-budget-form-group"),
        minimum: 10,
        isError: !1,
        value: b(".js-budget-input").val(),
        fee_percentage: b("#project-fee-percentage").val()
    },
    b(function() {
        i(),
        j()
    })
}(this, jQuery),
function(a, b, c) {
    var d = {
        DEFAULT_SCOPE: ".js-default-content",
        TABLE_SELECTOR: "table.js-table-sorter"
    }
      , e = function(a) {
        if (0 !== a.length) {
            var c = a.find("th").length - 1
              , d = a.find(".js-segment-toggle td")
              , e = 0;
            d.each(function(d, f) {
                e > c && (e = 0);
                var g = b(this).parent(".js-segment-toggle")
                  , h = a.find('.js-segment-collapse[data-project-target="' + g.data("project") + '"]')
                  , i = b(this).find(".js-table-sorter-ignore")
                  , j = b(this).text().trim();
                if (i.length) {
                    var k = b(this).clone();
                    k.find(".js-table-sorter-ignore").remove(),
                    j = k.text().trim()
                }
                g.attr("data-column-" + e, j),
                h.attr("data-column-" + e, j),
                e++
            });
            var f = function(a, c, d) {
                return b(a).parent("tr").data("column-" + d)
            }
            ;
            a.tablesorter({
                sortList: [[1, 1]],
                sortInitialOrder: "desc",
                textExtraction: f
            }),
            a.find(".js-segment-toggle").on("click", function(c) {
                var d = b(this)
                  , e = a.find(".js-segment-collapse")
                  , f = "closed"
                  , g = d.closest("table").find(".js-segment-toggle");
                if (e.addClass(f),
                d.hasClass("parent-closed")) {
                    var h = a.find('.js-segment-collapse[data-project-target="' + d.data("project") + '"]');
                    h.removeClass(f),
                    d.removeClass("parent-closed"),
                    g = g.not(d)
                }
                g.addClass("parent-closed")
            }).addClass("parent-closed")
        }
    }
      , f = function(a) {
        a.find(d.TABLE_SELECTOR).each(function() {
            e(b(this))
        })
    }
    ;
    b(function() {
        Crew.Utils.initializers.add(f),
        f(b(d.DEFAULT_SCOPE))
    })
}(window, jQuery),
function(a, b, c) {
    var d, e;
    e = {
        DEFAULT_SCOPE: ".js-default-content"
    },
    d = {},
    d.init = function() {
        this.registerDismissNoticeButtons()
    }
    ,
    d.registerDismissNoticeButtons = function() {
        b(e.DEFAULT_SCOPE).on("submit", "form.js-notice-save", function(a) {
            var c = b(this);
            c.closest(".js-notice").hide()
        })
    }
    ,
    b(function() {
        d.init()
    })
}(window, jQuery),
function(a, b, c) {
    var d = function(a, c, d) {
        this.$target = b(a),
        this.$anchor = b(c),
        this.$phantomAnchor = b(d)
    }
    ;
    d.prototype.relocate = function() {
        if (this.$anchor.length) {
            var c = b(a).scrollTop()
              , d = this.$anchor.offset().top;
            c > d ? (this.$target.addClass("is-sticky-on"),
            this.$phantomAnchor.show()) : (this.$target.removeClass("is-sticky-on"),
            this.$phantomAnchor.hide())
        }
    }
    ,
    b(function() {
        var c = new d(".js-sticky",".js-sticky-anchor",".js-sticky-phantom");
        c.relocate(),
        b(a).scroll(function(a) {
            c.relocate()
        })
    })
}(window, jQuery),
function(a, b, c) {
    var d, e, f, g, h, i;
    d = e = {},
    f = {},
    g = {},
    h = {},
    i = {
        expandedCSSClass: "row-expanded",
        expandedJSClass: ".js-expandable-row",
        notExpandedCaretClass: "icon-caret-right",
        expandedCaretClass: "icon-caret-down"
    },
    d.init = function() {
        this.$row = b(".js-expand-row"),
        this.$caret = b(".js-expand-row-caret")
    }
    ,
    f.shouldBeExpanded = function(a) {
        return !b(a).is("a, button, input")
    }
    ,
    f.toggle = function(a, b) {
        g.isOpen(a) ? this.close(a, b) : this.open(a, b)
    }
    ,
    f.open = function(a, b) {
        a.addClass(i.expandedCSSClass),
        b.removeClass(i.notExpandedCaretClass).addClass(i.expandedCaretClass)
    }
    ,
    f.close = function(a, b) {
        a.removeClass(i.expandedCSSClass),
        b.removeClass(i.expandedCaretClass).addClass(i.notExpandedCaretClass)
    }
    ,
    g.isOpen = function(a) {
        return a.hasClass(i.expandedCSSClass)
    }
    ,
    h.init = function() {
        this.listenToExpandEvent()
    }
    ,
    h.listenToExpandEvent = function() {
        e.$row.on("click", function(a) {
            var c = b(this);
            if (f.shouldBeExpanded(a.target)) {
                a.preventDefault();
                var d = c.next(i.expandedJSClass)
                  , g = c.find(e.$caret);
                f.toggle(d, g)
            }
        })
    }
    ,
    b(function() {
        d.init(),
        h.init()
    })
}(window, jQuery),
function(a, b, c) {
    var d, e, f, g, h;
    e = d = {},
    f = {},
    g = {
        DEFAULT_SCOPE: ".js-default-content",
        NOTIFICATIONS_CONTAINER_CLASS: ".js-notifications-container",
        UNSUBSCRIBE_CLASS: ".js-checkbox-unsubscribe",
        CHECKBOXES_CLASS: ".js-checkbox",
        REMINDER_CHECKBOX_CLASS: ".js-project-reminder-checkbox",
        REMINDER_CLASS: "js-project-reminder"
    },
    h = {},
    f.init = function() {
        var a = b(g.UNSUBSCRIBE_CLASS)
          , c = b(g.REMINDER_CHECKBOX_CLASS);
        a.length && f.updateNotificationsSettings(a[0].checked),
        c.length && f.updateProjectReminderSettings(c[0].checked)
    }
    ,
    f.updateNotificationsSettings = function(a) {
        var c = b(g.REMINDER_CHECKBOX_CLASS);
        c.length && (c = c[0].checked),
        a ? (b(g.NOTIFICATIONS_CONTAINER_CLASS).each(function() {
            var a = b(this);
            !c && a.hasClass(g.REMINDER_CLASS) || a.removeClass("deactivated")
        }),
        b(g.CHECKBOXES_CLASS).each(function() {
            this.disabled = !1
        })) : (b(g.NOTIFICATIONS_CONTAINER_CLASS).addClass("deactivated"),
        b(g.CHECKBOXES_CLASS).each(function() {
            this.checked = !1,
            this.disabled = !0
        }))
    }
    ,
    f.updateProjectReminderSettings = function(a) {
        a ? b("." + g.REMINDER_CLASS).removeClass("deactivated") : b("." + g.REMINDER_CLASS).addClass("deactivated")
    }
    ,
    h.init = function() {
        b(g.DEFAULT_SCOPE).on("change", g.UNSUBSCRIBE_CLASS, function() {
            f.updateNotificationsSettings(this.checked)
        }),
        b(g.DEFAULT_SCOPE).on("change", g.REMINDER_CHECKBOX_CLASS, function() {
            f.updateProjectReminderSettings(this.checked)
        })
    }
    ,
    b(function() {
        f.init(),
        h.init(),
        Crew.Utils.initializers.add(function() {
            f.init()
        })
    })
}(window, jQuery),
function(a, b, c) {
    var d = function(c) {
        var d, e, f, g, h, i;
        return d = e = {},
        f = {},
        g = {},
        h = {},
        i = {
            TARGET_SELECTOR: ".js-nav-item",
            ACTIVE_CLASS: "active"
        },
        d.init = function() {
            this.$el = c,
            this.$el.on("updateNeeded", function() {
                g.findAndSetActive(),
                f.$active.length && h.listenForTargetRegionUpdate()
            })
        }
        ,
        f.init = function() {
            this.$active = e.$el.find(i.TARGET_SELECTOR + "." + i.ACTIVE_CLASS),
            this.eventTokens = {},
            this.urls = {}
        }
        ,
        f.addToken = function(a, b) {
            var c = a.getId();
            f.eventTokens[c] ? f.eventTokens[c].tokens.push(b) : (f.eventTokens[c] = {},
            f.eventTokens[c].region = a,
            f.eventTokens[c].tokens = [b])
        }
        ,
        g.init = function() {
            f.$active.length || g.findAndSetActive()
        }
        ,
        g.findAndSetActive = function() {
            var a = g.findActive();
            a && (f.$active = a,
            a.addClass(i.ACTIVE_CLASS))
        }
        ,
        g.getRegionUrlFromUrl = function(c) {
            return b.deparam(decodeURIComponent(a.location.search.substr(1)))[c]
        }
        ,
        g.normalizeUrl = function(a) {
            var b = a.indexOf("?");
            return a.substring(0, -1 !== b ? b : a.length)
        }
        ,
        g.findActive = function() {
            var a = !1;
            return e.$el.find(i.TARGET_SELECTOR).each(function() {
                var c = b(this)
                  , d = 0
                  , e = c.data("reloadRegions")
                  , f = Object.keys(e);
                return f.forEach(function(a) {
                    var b;
                    "self" !== a && (b = Crew.Layout.getRegion(a)),
                    b && g.normalizeUrl(b.getUrl()) === e[a] && (d += 1)
                }),
                d === f.length ? (a = c,
                !1) : void 0
            }),
            a
        }
        ,
        g.toggleClasses = function(a) {
            a.hasClass(i.ACTIVE_CLASS) || (f.$active && f.$active.removeClass(i.ACTIVE_CLASS),
            f.$active = a,
            f.$active.addClass(i.ACTIVE_CLASS))
        }
        ,
        g.clear = function() {
            f.$active && f.$active.length && (f.$active.removeClass(i.ACTIVE_CLASS),
            f.$active = !1)
        }
        ,
        h.init = function() {
            g.init(),
            this.listenForChange(),
            f.$active.length && this.listenForTargetRegionUpdate()
        }
        ,
        h.setup = function() {
            h.unsubscribeFromAllEvents(),
            h.listenForTargetRegionUpdate()
        }
        ,
        h.listenForChange = function() {
            e.$el.on("click", i.TARGET_SELECTOR, function(a) {
                var c = b(this);
                g.toggleClasses(c),
                f.urls = c.data("reloadRegions"),
                h.setup()
            })
        }
        ,
        h.unsubscribeFromAllEvents = function() {
            _.each(f.eventTokens, function(a) {
                _.each(a.tokens, function(b) {
                    a.region.unsubscribe(b)
                })
            }),
            f.eventTokens = {}
        }
        ,
        h.listenForTargetRegionUpdate = function() {
            var a = h.getTargetRegion();
            return a.length ? void _.each(a, function(a) {
                var b, c = a.getId();
                b = a.subscribe("willUpdate", function() {
                    f.urls[c] && f.urls[c] === a.getUrl() || (g.clear(),
                    h.unsubscribeFromAllEvents())
                }),
                f.addToken(a, b)
            }) : !1
        }
        ,
        h.getTargetRegion = function() {
            var a = f.$active.data("reloadRegions")
              , b = [];
            return a && Object.keys(a).forEach(function(a) {
                var c;
                "self" !== a && (c = Crew.Layout.getRegion(a),
                c && b.push(c))
            }),
            b
        }
        ,
        h.teardown = function() {
            e.$el.off("click"),
            e.$el = null ,
            f.$active = null ,
            f.urls = null ,
            h.unsubscribeFromAllEvents()
        }
        ,
        d.init(),
        f.init(),
        h.init(),
        {
            teardown: h.teardown
        }
    }
      , e = Crew.Utils.moduleManager(".js-nav-items", d);
    Crew.Utils.initializers.add(function(a) {
        e.create(a)
    }),
    Crew.Utils.terminators.add(function(a) {
        e.remove(a)
    }),
    b(function() {
        e.create(b(".js-default-content"))
    })
}(window, jQuery);
var Crew = Crew || {};
Crew.ProjectUsers = Crew.ProjectUsers || {},
Crew.User = Crew.User || {},
function(a, b, c) {
    var d, e, f, g;
    e = d = {},
    f = {},
    g = {
        CONTAINER_SELECTOR: ".js-project-users",
        ONLINE_CLASS: "online",
        OFFLINE_CLASS: "offline"
    },
    f.init = function(a) {
        f.setOnline(Crew.Pusher.Channels.getAllActiveUsers()),
        f.setOnline([{
            user_id: Crew.User.id
        }])
    }
    ,
    f.getUserElement = function(a, b) {
        return b.find('[data-user-id="' + a.user_id + '"]')
    }
    ,
    f.process = function(a, c) {
        var d = b(g.CONTAINER_SELECTOR);
        d.length && _.each(a, function(a) {
            var b = f.getUserElement(a, d);
            b && b.length && c(b)
        })
    }
    ,
    f.setOnline = function(a) {
        f.process(a, function(a) {
            a.removeClass(g.OFFLINE_CLASS),
            a.addClass(g.ONLINE_CLASS),
            f.replaceInTooltip(a, "offline", "online")
        })
    }
    ,
    f.setOffline = function(a) {
        f.process(a, function(a) {
            a.removeClass(g.ONLINE_CLASS),
            a.addClass(g.OFFLINE_CLASS),
            f.replaceInTooltip(a, "online", "offline")
        })
    }
    ,
    f.replaceInTooltip = function(a, b, c) {
        var d = a.attr("data-original-title");
        "undefined" != typeof d ? a.attr("data-original-title", d.replace(b, c)) : a.attr("title", a.attr("title").replace(b, c))
    }
    ,
    b(function() {
        f.init(b(".js-default-content"))
    }),
    Crew.Utils.initializers.add(function(a) {
        f.init(a)
    });
    var h = {
        setOnline: f.setOnline,
        setOffline: f.setOffline
    };
    b.extend(Crew.ProjectUsers, h)
}(window, jQuery);
var Crew = Crew || {};
Crew.Pusher = Crew.Pusher || {},
Crew.Pusher.Channels = Crew.Pusher.Channels || {},
function(a, b, c) {
    var d, e, f, g, h, i = {};
    d = e = {},
    f = {},
    g = {},
    h = {
        DEFAULT_SCOPE: ".js-default-content",
        NEW_MESSAGE_FORM: ".js-new-message form",
        MESSAGE_CONTAINER: ".js-load-more",
        CLEAR_NOTIFICATION_FORM: ".js-clear-channel-notification form",
        EVENTS: {
            NEW_UPLOAD: "new_upload",
            NEW_MESSAGE: "new_message",
            PUSHER_SUBSCRIPTION_SUCCEEDED: "pusher:subscription_succeeded",
            PUSHER_MEMBER_ADDED: "pusher:member_added",
            PUSHER_MEMBER_REMOVED: "pusher:member_removed"
        },
        CHANNEL_ATTR: "[data-pusher-channel]"
    },
    f.getChannels = function(a) {
        var c;
        return c = "undefined" == typeof a ? b(h.DEFAULT_SCOPE).find(h.CHANNEL_ATTR) : a.find(h.CHANNEL_ATTR)
    }
    ,
    f.initializeChannel = function(a, b) {
        i.hasOwnProperty(a) && null  !== i[a] ? g.sendInitializeEvents(a) : (i[a] = {},
        i[a].channel = pusher.subscribe(a),
        g.bindToChannelEvents(a)),
        i[a].$el = b,
        i[a].isScrollable = !1,
        i[a].$scrollable = b.closest(".js-scrollable"),
        i[a].$container = b.find(h.MESSAGE_CONTAINER),
        i[a].$clearNotificationsForm = b.find(h.CLEAR_NOTIFICATION_FORM),
        i[a].virtual = !1
    }
    ,
    f.unsubscribeFromChannel = function(a) {
        i[a].channel.unsubscribe(),
        i[a] = null 
    }
    ,
    f.presentLatestMessages = function(a) {
        var b = (i[a].$container,
        i[a].$scrollable);
        b.length && b.animate({
            scrollTop: b[0].scrollHeight
        }, 300)
    }
    ,
    g.channelSequence = function(a, c) {
        var d = f.getChannels(a);
        d.length && d.each(function() {
            var a = b(this)
              , d = a.data("pusherChannel");
            d && c(d, a)
        })
    }
    ,
    g.init = function(a) {
        g.listenToNewMessageFormSubmit(a),
        g.channelSequence(a, function(a, b) {
            f.initializeChannel(a, b)
        })
    }
    ,
    g.sleep = function(a) {
        g.channelSequence(a, function(a) {
            i[a].virtual = !0
        })
    }
    ,
    g.terminate = function(a) {
        g.channelSequence(a, function(a) {
            g.unbindFromChannelEvents(a),
            f.unsubscribeFromChannel(a),
            i[a] = null 
        })
    }
    ,
    g.listenToNewMessageFormSubmit = function(a) {
        var b = a.find(h.NEW_MESSAGE_FORM);
        b.on("submit", function(a) {
            var c = _.object(_.map(b.serializeArray(), function(a) {
                return [a.name, a.value]
            }));
            c.body = Crew.Utils.html.htmlentities(c.body),
            c.body = Crew.Utils.html.autop(c.body),
            c.body = Crew.Utils.html.autolink(c.body, "_blank"),
            Crew.Messages.insertReceivedMessage(c, i[c.channel_name].$el, !0),
            f.presentLatestMessages(c.channel_name)
        })
    }
    ,
    g.bindToChannelEvents = function(a) {
        var b = i[a].channel;
        b.bind(h.EVENTS.NEW_UPLOAD, function(b) {
            i[a].virtual || (Crew.Uploader.insertReceivedUpload(b, i[a].$el),
            f.presentLatestMessages(a),
            b.from_user_id !== Crew.User.id && Crew.Ajax.Form.submit(i[a].$clearNotificationsForm))
        }),
        b.bind(h.EVENTS.NEW_MESSAGE, function(b) {
            i[a].virtual || (Crew.Messages.insertReceivedMessage(b, i[a].$el),
            f.presentLatestMessages(a),
            b.from_user_id !== Crew.User.id && Crew.Ajax.Form.submit(i[a].$clearNotificationsForm))
        }),
        b.bind("pusher:subscription_succeeded", function(a) {
            Crew.ProjectUsers.setOnline(_.values(a.members))
        }),
        b.bind("pusher:member_added", function(a) {
            Crew.ProjectUsers.setOnline([a.info])
        }),
        b.bind("pusher:member_removed", function(a) {
            Crew.ProjectUsers.setOffline([a.info])
        })
    }
    ,
    g.sendInitializeEvents = function(a) {
        Crew.ProjectUsers.setOnline(_.values(i[a].channel.members.members))
    }
    ,
    g.unbindFromChannelEvents = function(a) {
        _.each(h.EVENTS, function(b) {
            i[a].channel.unbind(b)
        })
    }
    ,
    g.getAllActiveUsers = function() {
        var a = [];
        return _.each(i, function(b) {
            Array.prototype.push.apply(a, _.values(b.channel.members.members))
        }),
        a
    }
    ,
    b(function() {
        var a = b(h.DEFAULT_SCOPE);
        g.init(a),
        Crew.Utils.initializers.add(function(a) {
            g.init(a)
        }),
        Crew.Utils.terminators.add(function(a) {
            g.sleep(a)
        })
    });
    var j = {
        getAllActiveUsers: g.getAllActiveUsers
    };
    b.extend(Crew.Pusher.Channels, j)
}(window, jQuery);
var Crew = Crew || {};
Crew.Ajax = Crew.Ajax || {},
Crew.Ajax.Form = Crew.Ajax.Form || {},
function(a, b, c) {
    var d, e, f, g, h, i, j = Crew.Utils.eventify({});
    d = e = {},
    f = {},
    g = {},
    h = {},
    i = {
        PROCESS_MESSAGE_SAVING: "Saving...",
        RESULT_MESSAGE_CLASS: ".js-result-message",
        SAVE_BUTTON_CLASS: ".js-save-button",
        ERROR_MESSAGE_JS_CLASS: "js-form-error-message",
        ERROR_FORM_GROUP_CLASS: "has-error"
    },
    f.showSavingMessage = function(a) {
        var b;
        f.disableSaveButton(a),
        b = a.find(i.RESULT_MESSAGE_CLASS),
        b.removeClass("text-danger"),
        b.addClass("text-muted"),
        b.html(f.getSavingMessage(a))
    }
    ,
    f.getSavingMessage = function(a) {
        var b = f.getRequestMessage(a, "processing");
        return b === c ? i.PROCESS_MESSAGE_SAVING : b + "..."
    }
    ,
    f.getRequestMessage = function(a, b) {
        return a.data("message-" + b)
    }
    ,
    f.processFailure = function(a) {
        var b = a.find(i.RESULT_MESSAGE_CLASS);
        b.addClass("text-danger"),
        b.html(f.getRequestMessage(a, "failure"))
    }
    ,
    f.showSuccessMessage = function(a) {
        var b = a.find(i.RESULT_MESSAGE_CLASS);
        b.removeClass("text-danger"),
        b.html(f.getRequestMessage(a, "success"))
    }
    ,
    f.removeErrorFromField = function(a) {
        a.removeClass(i.ERROR_FORM_GROUP_CLASS),
        a.find("." + i.ERROR_MESSAGE_JS_CLASS).remove()
    }
    ,
    f.addErrorToField = function(a, b) {
        a.addClass(i.ERROR_FORM_GROUP_CLASS),
        a.append('<span class="' + i.ERROR_MESSAGE_JS_CLASS + ' text-danger text-size-xs">' + b + "</span>")
    }
    ,
    f.showErrorMessage = function(a, c) {
        var d, e;
        d = a.find(i.RESULT_MESSAGE_CLASS),
        d.addClass("text-danger"),
        d.html(f.getRequestMessage(a, "error"));
        try {
            e = b.parseJSON(c)
        } catch (g) {
            return void (isNaN(c) && d.html(c))
        }
        b.each(e, function(b, c) {
            var d = a.find('[name="' + b + '"]')
              , e = d.closest(".form-group");
            f.removeErrorFromField(e),
            c.forEach(function(a) {
                f.addErrorToField(e, a)
            })
        })
    }
    ,
    f.reset = function(a) {
        a.find("input[type=text], textarea").val(""),
        f.clearResultMessage(a)
    }
    ,
    f.clearResultMessage = function(a) {
        var b = a.find(i.RESULT_MESSAGE_CLASS);
        b.removeClass("text-danger"),
        b.html("")
    }
    ,
    f.enableSaveButton = function(a) {
        a.find(i.SAVE_BUTTON_CLASS).removeAttr("disabled")
    }
    ,
    f.disableSaveButton = function(a) {
        a.find(i.SAVE_BUTTON_CLASS).attr("disabled", "disabled")
    }
    ,
    g.closestModalWillClose = function(a) {
        return a.data("close-modal")
    }
    ,
    h.init = function() {
        this.listenForTyping(),
        this.listenForSave()
    }
    ,
    h.listenForTyping = function() {
        b(".js-default-content").on("keyup change", ".js-ajax-field:not(.js-ajax-field-ignore)", function(a) {
            if (13 !== a.keyCode) {
                var c = b(a.target).closest(".js-ajax-form");
                f.enableSaveButton(c),
                f.clearResultMessage(c);
                var d = b(a.target).closest(".form-group");
                f.removeErrorFromField(d)
            }
        })
    }
    ,
    h.submit = function(d, e) {
        var h = function(a) {
            return a.closest(".js-region").data("regionId")
        }
        ;
        f.showSavingMessage(d);
        var i = b("body").first().data("csrfToken");
        b.ajax({
            url: d.attr("action"),
            type: d.attr("method"),
            headers: {
                "X-CSRF-Token": i
            },
            data: d.serialize(),
            success: function(i) {
                var k, l, m = [];
                if ("1" == i) {
                    var n = d.data("redirect-on-success");
                    d.hasClass("js-form-block-add") && f.reset(d),
                    n !== c && (a.location = n),
                    g.closestModalWillClose(d) && (k = d.closest(".modal.fade.in"),
                    k.length && (k.modal("hide"),
                    b(document).find(".modal-backdrop.fade.in").remove(),
                    b("body").removeClass("modal-open"))),
                    d.hasClass("js-reload-region") && (l = d.data("reload-region"),
                    "undefined" == typeof l ? m.push(Crew.Layout.getRegion(h(d))) : _.each(_.uniq(l.split(",")), function(a) {
                        m.push(Crew.Layout.getRegion("self" === a ? h(d) : a))
                    }),
                    _.each(m, function(a) {
                        a.update()
                    })),
                    f.showSuccessMessage(d),
                    "function" == typeof e && e(),
                    j.publish("submitSuccess", d)
                } else
                    f.showErrorMessage(d, i),
                    j.publish("submitFailure", d)
            },
            error: function(a, b, c) {
                f.processFailure(d)
            }
        })
    }
    ,
    h.listenForSave = function() {
        b(".js-default-content").on("submit", ".js-ajax-form", function(a) {
            a.preventDefault(),
            Crew.Ajax.Form.submit(b(a.target))
        })
    }
    ,
    b(function() {
        h.init()
    }),
    b.extend(a.Crew.Ajax.Form, b.extend(j, {
        submit: h.submit,
        reset: f.reset,
        showSavingMessage: f.showSavingMessage,
        showErrorMessage: f.showErrorMessage,
        showSuccessMessage: f.showSuccessMessage,
        processFailure: f.processFailure,
        enableSaveButton: f.enableSaveButton,
        removeErrorFromField: f.removeErrorFromField
    }))
}(window, jQuery),
function(a, b, c) {
    var d, e, f;
    d = {},
    e = {},
    f = {
        DEFAULT_SCOPE: ".js-default-content",
        COLLAPSE_CONTAINER: ".js-collapse-container",
        COLLAPSE_ACTION: ".js-collapse-action",
        FORM_BLOCK_ADD: "js-ajax-form-block-add",
        FORM_BLOCK_CLOSE: "js-ajax-form-block-close"
    },
    d.resetAddForm = function(a) {
        a.closest(".js-collapse-container").find(".js-collapse-action").first().trigger("click"),
        Crew.Ajax.Form.reset(a),
        Crew.Ajax.Form.enableSaveButton(a)
    }
    ,
    d.resetAndCloseForm = function(a) {
        Crew.Ajax.Form.reset(a),
        Crew.Ajax.Form.enableSaveButton(a),
        $closest = a.closest(f.COLLAPSE_CONTAINER),
        $closest.find(".js-collapse-full").hide(),
        $closest.find(".js-shortened").show()
    }
    ,
    e.init = function() {
        this.listenForSubmit()
    }
    ,
    e.listenForSubmit = function() {
        Crew.Ajax.Form.subscribe("submitSuccess", function(a) {
            a.hasClass(f.FORM_BLOCK_ADD) && d.resetAddForm(a),
            a.hasClass(f.FORM_BLOCK_CLOSE) && d.resetAndCloseForm(a)
        })
    }
    ,
    b(function() {
        e.init()
    })
}(window, jQuery);
var Crew = Crew || {};
Crew.Ajax = Crew.Ajax || {},
Crew.Ajax.LoadMore = Crew.Ajax.LoadMore || {},
function(a, b, c) {
    var d, e, f, g, h, i;
    d = e = {},
    f = {},
    g = {},
    h = {},
    i = {
        UPLOADER_JS_CLASS: ".js-uploader-single",
        CONTAINER_JS_CLASS: ".js-load-more",
        DESTINATION_JS_CLASS: ".js-load-more-hole",
        TRIGGER_JS_CLASS: ".js-load-more-btn"
    },
    d.init = function() {
        this.$container = b(i.CONTAINER_JS_CLASS)
    }
    ,
    f.init = function() {
        f.initScrolling()
    }
    ,
    f.showLoadingMessage = function(a) {
        var b = a.find(".js-load-more-loading");
        b ? b.show() : f.appendNewContent(a, '<div class="js-load-more-loading">Loading more goodness</div>', !1)
    }
    ,
    f.appendNewContent = function(a, b, c, d) {
        var e, f, g = a.find(i.DESTINATION_JS_CLASS), h = !1;
        d ? g.html(b) : "reverse" === a.data("order") ? (a.data("scrollable") && (h = !0,
        e = a.closest(".js-scrollable"),
        f = a[0].scrollHeight),
        g.prepend(b),
        h && a.scrollTop(e[0].scrollHeight - f)) : g.append(b),
        c && a.find(".js-load-more-loading").remove()
    }
    ,
    h.init = function() {
        this.listenForLoadMore(),
        this.listenForReset()
    }
    ,
    h.resetToStart = function() {}
    ,
    f.initScrolling = function(a) {
        a = a || e.$container,
        a.each(function() {
            var a = b(this);
            a.data("scrollable") && "reverse" === a.data("order") && Crew.Utils.scroll.toBottom(a.closest(".js-region"))
        })
    }
    ,
    h.doLoad = function(a) {
        var b = a.data("url");
        if (b)
            return h.loadMore(a, b, !0)
    }
    ,
    h.listenForReset = function() {
        b(".js-default-content").on("Crew:loadMoreReset", i.CONTAINER_JS_CLASS, function(a) {
            h.doLoad(b(this))
        })
    }
    ,
    h.listenForLoadMore = function() {
        b(".js-default-content").on("click", i.TRIGGER_JS_CLASS, function(a) {
            var c, d, e;
            a.preventDefault(),
            c = b(a.target),
            c.hide(),
            d = b(a.target).closest(i.CONTAINER_JS_CLASS),
            e = h.loadMore(d, c.data("url"), !1)
        })
    }
    ,
    h.loadMore = function(a, d, e) {
        return typeof d !== c ? (f.showLoadingMessage(a),
        b.ajax({
            url: d,
            type: "GET"
        }).then(function(b) {
            f.appendNewContent(a, b, !0, e)
        }).fail(function() {
            console.log("Could not load more something")
        })) : void 0
    }
    ,
    b(function() {
        d.init(),
        h.init(),
        f.init(),
        Crew.Utils.initializers.add(function(a) {
            var c = a.find(i.CONTAINER_JS_CLASS);
            c.length && c.each(function() {
                f.initScrolling(b(this))
            })
        })
    })
}(window, jQuery),
function(a, b, c) {
    var d, e, f, g, h, i;
    d = e = {},
    f = {},
    g = {},
    h = {},
    i = {
        YES_BUTTON_CLASS: ".js-btn-yes",
        NO_BUTTON_CLASS: ".js-btn-no",
        MODAL_TITLE_CLASS: ".js-modal-title",
        MODAL_BODY_CLASS: ".js-modal-body"
    },
    d.init = function() {
        this.$modal = b(".js-confirm-modal"),
        this.$confirmButton = b(".js-confirm")
    }
    ,
    f.updateModal = function(a) {
        var b, c, d, f;
        b = a.data("confirm-title"),
        c = a.data("confirm-text"),
        d = a.data("confirm-yes"),
        f = a.data("confirm-no"),
        e.$modal.find(i.MODAL_TITLE_CLASS).html(b),
        e.$modal.find(i.MODAL_BODY_CLASS).html(c),
        e.$modal.find(i.YES_BUTTON_CLASS).html(d),
        e.$modal.find(i.NO_BUTTON_CLASS).html(f)
    }
    ,
    g.setForm = function(a) {
        g.$form = a
    }
    ,
    h.init = function() {
        this.listenForConfirmButton(),
        this.listenForYesButton(),
        this.listenForNoButton()
    }
    ,
    h.listenForConfirmButton = function() {
        b(".js-default-content").on("click", ".js-confirm", function(a) {
            a.preventDefault();
            var c = b(a.target);
            g.setForm(c.closest("form")),
            f.updateModal(b(this)),
            e.$modal.modal({
                show: !0
            })
        })
    }
    ,
    h.listenForYesButton = function() {
        b(i.YES_BUTTON_CLASS).click(function(a) {
            Crew.Ajax.Form.submit(g.$form),
            e.$modal.modal("hide")
        })
    }
    ,
    h.listenForNoButton = function() {
        b(i.NO_BUTTON_CLASS).click(function(a) {
            g.setForm(""),
            e.$modal.modal("hide")
        })
    }
    ,
    b(function() {
        d.init(),
        h.init()
    })
}(window, jQuery);
var Crew = Crew || {};
Crew.Costs = Crew.Costs || {},
Crew.Utils = Crew.Utils || {},
Crew.Utils.Money = Crew.Utils.Money || {},
function(a, b, c) {
    var d, e, f, g, h, i;
    d = e = {},
    f = {},
    g = {},
    h = {},
    i = {
        TYPE_RADIO_BUTTON_CLASS: ".js-cost-calculator-type:checked",
        CALCULATOR_CLASS: ".js-cost-calculator",
        FIXED_ROW_CLASS: ".js-cost-calculator-fixed",
        HOURLY_ROW_CLASS: ".js-cost-calculator-hourly",
        HOURS_INPUT_CLASS: ".js-cost-calculator-hours",
        HOURLY_RATE_INPUT_CLASS: ".js-cost-calculator-hourly-rate",
        DEV_COST_INPUT_CLASS: ".js-cost-calculator-dev-cost",
        PO_COST_INPUT_CLASS: ".js-cost-calculator-po-cost",
        FEE_CLASS: ".js-cost-calculator-fee"
    },
    d.init = function() {
        this.$content = b(".js-default-content"),
        this.$radios = b(i.TYPE_RADIO_BUTTON_CLASS),
        this.$calculators = b(i.CALCULATOR_CLASS)
    }
    ,
    f.init = function() {
        e.$radios.each(function() {
            f.updateRows(b(this))
        }),
        e.$calculators.each(function() {
            f.updateCost(b(this))
        })
    }
    ,
    f.updateRows = function(a) {
        var b = a.val();
        "fixed" == b ? (a.closest(i.CALCULATOR_CLASS).find(i.FIXED_ROW_CLASS).show(),
        a.closest(i.CALCULATOR_CLASS).find(i.HOURLY_ROW_CLASS).hide()) : (a.closest(i.CALCULATOR_CLASS).find(i.FIXED_ROW_CLASS).hide(),
        a.closest(i.CALCULATOR_CLASS).find(i.HOURLY_ROW_CLASS).show())
    }
    ,
    f.updateCost = function(a) {
        var b, c, d, e, f, g, h = a.data("fee-percentage"), j = (a.data("role"),
        a.find(i.TYPE_RADIO_BUTTON_CLASS));
        b = 0 === j.length ? "fixed" : j.val(),
        "fixed" == b ? (c = a.find(i.DEV_COST_INPUT_CLASS).val(),
        d = c / (1 - h)) : (e = a.find(i.HOURS_INPUT_CLASS).val(),
        f = a.find(i.HOURLY_RATE_INPUT_CLASS).val(),
        d = f * e / (1 - h)),
        g = a.find(i.PO_COST_INPUT_CLASS),
        g.html(Crew.Utils.Money.formatCurrency(d));
        var k = a.find(i.FEE_CLASS)
          , l = Crew.Utils.Money.formatCurrency(d * h);
        k.html(l)
    }
    ,
    h.init = function() {
        this.listenForTypeChange(),
        this.listenForCostChange()
    }
    ,
    h.listenForTypeChange = function() {
        e.$content.on("click", i.TYPE_RADIO_BUTTON_CLASS, function(a) {
            var c = b(a.target);
            f.updateRows(c);
            var d = b(a.target).closest(i.CALCULATOR_CLASS);
            f.updateCost(d)
        })
    }
    ,
    h.listenForCostChange = function() {
        e.$content.on("keyup change", i.DEV_COST_INPUT_CLASS, function(a) {
            var c = b(a.target).closest(i.CALCULATOR_CLASS);
            f.updateCost(c)
        }),
        e.$content.on("keyup change", i.PO_COST_INPUT_CLASS, function(a) {
            var c = b(a.target).closest(i.CALCULATOR_CLASS);
            f.updateCost(c)
        }),
        e.$content.on("keyup change", i.HOURS_INPUT_CLASS, function(a) {
            var c = b(a.target).closest(i.CALCULATOR_CLASS);
            f.updateCost(c)
        }),
        e.$content.on("keyup change", i.HOURLY_RATE_INPUT_CLASS, function(a) {
            var c = b(a.target).closest(i.CALCULATOR_CLASS);
            f.updateCost(c)
        })
    }
    ,
    h.setupAjaxCalcs = function() {
        d.init(),
        f.init()
    }
    ,
    b(function() {
        d.init(),
        h.init(),
        f.init(),
        Crew.Utils.initializers.add(function(a) {
            h.setupAjaxCalcs()
        })
    });
    var j = {
        init: h.setupAjaxCalcs
    };
    b.extend(a.Crew.Costs, j)
}(window, jQuery),
function(a, b, c) {
    var d = Crew.Utils.moduleManager(".js-enter-to-submit", Crew.Modules.ets);
    Crew.Utils.initializers.add(function(a) {
        d.create(a)
    }),
    Crew.Utils.terminators.add(function(a) {
        d.remove(a)
    }),
    b(function() {
        d.create(b(".js-default-content"))
    })
}(window, jQuery);
var Crew = Crew || {};
Crew.Uploader = Crew.Uploader || {},
function(a, b, c) {
    var d, e, f, g, h, i;
    d = e = {},
    f = {},
    g = {},
    h = {},
    i = {
        DEFAULT_SCOPE_CLASS: ".js-default-content",
        UPLOADER_JS_CLASS: ".js-uploader-single",
        FILEPICKER_JS_CLASS: ".js-filepicker",
        S3_BASE: "https://s3.amazonaws.com/ooomf-com-files/"
    },
    d.init = function() {
        this.$uploader = b(i.UPLOADER_JS_CLASS),
        this.$defaultScope = b(i.DEFAULT_SCOPE_CLASS)
    }
    ,
    f.init = function() {}
    ,
    h.init = function() {}
    ,
    f.insertReceivedUpload = function(a, b) {
        var c, d, f;
        b = b || e.$defaultScope,
        c = b.find(".js-upload-template"),
        d = c.find(".js-upload-link"),
        d.attr("href", a.url),
        d.html(a.name),
        Crew.Messages.setTemplateValues(c, a),
        f = b.find(".js-load-more-hole"),
        f.append(c.html()),
        Crew.Messages.hideNewMessagesLine()
    }
    ,
    h.process = function(a) {
        var c, d, e;
        c = b(a.target).closest(".js-uploader-single"),
        c.find("img").attr("src", a.fpfile.url),
        c.find("input[name=url]").val(a.fpfile.key),
        c.find("input[name=name]").val(a.fpfile.filename),
        c.find("input[name=content_type]").val(a.fpfile.mimetype),
        c.find("input[name=size]").val(a.fpfile.size),
        c.find("input[name=thumb_url]").val(a.fpfile.url),
        d = c.find("input[name=file_picker_url]"),
        d.val(a.fpfile.url),
        d.trigger("change"),
        e = c.closest(".js-ajax-form"),
        Crew.Ajax.Form.submit(e)
    }
    ,
    b(function() {
        d.init(),
        h.init(),
        f.init()
    });
    var j = {
        process: h.process,
        insertReceivedUpload: f.insertReceivedUpload
    };
    b.extend(a.Crew.Uploader, j)
}(window, jQuery);
var Crew = Crew || {};
Crew.Messages = Crew.Messages || {},
function(a, b, c) {
    var d, e, f, g, h, i;
    d = e = {},
    f = {},
    g = {},
    h = {},
    i = {
        DEFAULT_SCOPE_CLASS: ".js-default-content",
        UPLOADER_JS_CLASS: ".js-uploader-single"
    },
    d.init = function() {
        this.$defaultScope = b(i.DEFAULT_SCOPE_CLASS)
    }
    ,
    f.init = function() {}
    ,
    f.hideNewMessagesLine = function() {
        b(".js-channel-unread").css("opacity", 0)
    }
    ,
    f.setTemplateValues = function(a, b) {
        var c;
        a.find(".js-message-body").html(b.body),
        a.find(".js-from-user-name").html(b.from_user_first_name),
        a.find(".js-from-user-time").html(b.time),
        b.avatar_url ? (a.find(".js-avatar").attr("src", b.avatar_url).css("display", "block"),
        a.find(".js-default-avatar").css("display", "none")) : (a.find(".js-avatar").css("display", "none"),
        a.find(".js-default-avatar").html(h.getUserInitialsFromData(b)).css("display", "block")),
        c = b.forced ? "addClass" : "removeClass",
        a.find(".js-chat-module")[c]("pending")
    }
    ,
    f.unsetLastPendingMessageStatus = function(a) {
        a.find(".js-chat-module.pending").first().removeClass("pending")
    }
    ,
    f.insertReceivedMessage = function(a, c, d) {
        var g, h, i, j, k, l;
        d = "undefined" != typeof d,
        a.forced = d,
        c = c || e.$defaultScope,
        h = c.find(".js-load-more-hole");
        var m = b("[data-user-timezone]");
        return a.user_timezone = m.data("user-timezone"),
        d || a.from_user_id != Crew.User.id ? (d && a.from_user_id == Crew.User.id ? (k = moment(),
        l = moment.tz(k, a.user_timezone).format("h:mm A"),
        a.time = l) : d || a.from_user_id == Crew.User.id || (a.time = a.local[Crew.User.id].time,
        a.date = a.local[Crew.User.id].date),
        g = c.find(".js-message-template"),
        i = h.find(".js-from-user-date").last(),
        i.html() !== a.date && (j = i.clone(),
        j.html(a.date),
        h.append(j)),
        f.setTemplateValues(g, a),
        h.append(g.html()),
        void (a.from_user_id == Crew.User.id && f.hideNewMessagesLine())) : void f.unsetLastPendingMessageStatus(h)
    }
    ,
    h.getUserInitialsFromData = function(a) {
        return ["from_user_first_name", "from_user_last_name"].reduce(function(b, c) {
            return a[c] && b.push(a[c][0]),
            b
        }, []).join("").toUpperCase()
    }
    ,
    b(function() {
        d.init(),
        f.init()
    });
    var j = {
        getChannelElement: f.getChannelElement,
        insertReceivedMessage: f.insertReceivedMessage,
        hideNewMessagesLine: f.hideNewMessagesLine,
        setTemplateValues: f.setTemplateValues
    };
    b.extend(a.Crew.Messages, j)
}(window, jQuery);
var Crew = Crew || {};
Crew.Notifications = Crew.Notifications || {},
function(a, b, c) {
    var d, e, f, g, h, i, j = Crew.Utils.eventify({});
    d = e = {},
    f = {},
    g = {},
    h = {},
    i = {
        DATASOURCE_URL: "/notifications/all.json",
        PROJECT_SUPPORT_SELECTOR: ".js-project-support",
        USER_SUPPORT_SELECTOR: ".js-user-support",
        NOTIFICATION_CLASSES: "has-message has-support-message has-no-message has-no-support-message has-task-due",
        EVENTS: {
            NEW_NOTIFICATION: "new_notifications"
        }
    },
    d.init = function() {
        var a = b("[data-notifications-channel]");
        a.length && (this.$defaultContent = b(".js-default-content"),
        this.channel = a.data("notificationsChannel"),
        this.$projectSupport = {},
        this.$projectSupport.findElement = function(a) {
            return []
        }
        ,
        this.$userSupport = {},
        this.$userSupport.findElement = function(a) {
            return e.$defaultContent.find(i.USER_SUPPORT_SELECTOR)
        }
        ,
        this.add(this.$defaultContent))
    }
    ,
    d.add = function(a) {
        a.find(i.PROJECT_SUPPORT_SELECTOR).each(function() {
            var a = b(this);
            e.$projectSupport[a.data("projectId")] = a
        })
    }
    ,
    d.remove = function(a) {
        a.find(i.PROJECT_SUPPORT_SELECTOR).each(function() {
            var a = b(this).data("projectId");
            e.$projectSupport[a] && delete e.$projectSupport[a]
        })
    }
    ,
    g.init = function() {
        g.previousProjectStatus = {}
    }
    ,
    f.getProjectElement = function(a, b) {
        var c;
        return a.hasOwnProperty(b) || (c = a.findElement(b),
        c.length && (a[b] = c)),
        a[b]
    }
    ,
    f.setStatus = function(a, b, c) {
        var d = f.getProjectElement(a, b);
        d && d.length && (d.removeClass(i.NOTIFICATION_CLASSES),
        d.addClass(c["class"]))
    }
    ,
    f.setMultipleStatus = function(a) {
        Object.keys(a).forEach(function(b) {
            f.setStatus(e.$projectSupport, b, a[b])
        })
    }
    ,
    h.init = function() {
        d.channel = pusher.subscribe(e.channel),
        e.channel.bind(i.EVENTS.NEW_NOTIFICATION, h.reloadNotifications),
        Crew.Layout.subscribe("regionUpdated", function() {
            h.reloadNotifications()
        })
    }
    ,
    h.getNotifications = function() {
        return b.ajax({
            url: i.DATASOURCE_URL
        })
    }
    ,
    h.reloadNotifications = function(a) {
        h.getNotifications().then(function(a) {
            j.publish("ping", a),
            a.hasOwnProperty("projects") && a.projects.hasOwnProperty("status") && (f.setMultipleStatus(a.projects.status),
            f.setStatus(e.$userSupport, Crew.User.id, a.userSupport),
            g.previousProjectStatus = a.projects.status)
        })
    }
    ,
    b(function() {
        d.init(),
        e.channel && (g.init(),
        h.init())
    }),
    Crew.Utils.initializers.add(function(a) {
        d.add(a)
    }),
    Crew.Utils.terminators.add(function(a) {
        d.remove(a)
    }),
    b.extend(a.Crew.Notifications, b.extend(j, {
        reloadNotifications: h.reloadNotifications,
        classes: i.NOTIFICATION_CLASSES
    }))
}(window, jQuery),
function(a, b, c) {
    var d, e, f, g, h, i;
    d = e = {},
    f = {},
    g = {},
    h = {},
    i = {
        ATTRIBUTES_SPREADING_JS_CLASS: ".js-attributes-spreading",
        DATA_SPREADING_ATTRIBUTE: "spreading-values",
        DEFAULT_SCOPE: ".js-default-content"
    },
    d.init = function() {
        this.$scope = b(i.DEFAULT_SCOPE)
    }
    ,
    f.processTargetOptions = function(a) {
        var d = a.data(i.DATA_SPREADING_ATTRIBUTE);
        _.each(d, function(a, d) {
            a = jQuery.extend({}, a),
            a.content !== c && (b(d).text(a.content),
            a.content = null ),
            b(d).attr(a)
        })
    }
    ,
    h.init = function() {
        this.listenForClickEvent()
    }
    ,
    h.listenForClickEvent = function() {
        d.$scope.on("click", i.ATTRIBUTES_SPREADING_JS_CLASS, function(a) {
            f.processTargetOptions(b(this))
        })
    }
    ,
    b(function() {
        d.init(),
        h.init()
    })
}(window, jQuery),
function(a, b, c) {
    var d, e, f, g, h;
    d = e = {},
    f = {},
    g = {
        searchFieldCSSClass: ".js-algolia-search",
        apiKeyAttribute: "data-algolia-search-public-api-key",
        clientIdAttribute: "data-algolia-search-client-id",
        indexPrefixAttribute: "data-algolia-search-index-prefix",
        hiddenClass: "hidden-xs-up",
        indices: {
            projects: "Projects",
            messages: "Messages",
            uploads: "Uploads",
            jobs: "FAQs"
        },
        attributesToSnippet: ["description"]
    },
    h = {
        DEFAULT_SCOPE_CLASS: ".js-default-content",
        RESULT_TEMPLATE_SELECTOR: ".search-result-template",
        SEARCH_RESULT_SELECTOR: ".js-search-results",
        PROJECTS_LIST_SELECTOR: ".js-region",
        CONTENT_SELECTOR: ".sidebar-content",
        ERROR_TEMPLATE: '<div class="alert alert-danger">{{message}}</div>',
        SNIPPET_SIZE: 20,
        RESULT_LIMIT: 10
    },
    d.init = function() {
        var a = b(g.searchFieldCSSClass);
        a.length && (this.apiKey = a.attr(g.apiKeyAttribute),
        this.clientId = a.attr(g.clientIdAttribute),
        this.indexPrefix = a.attr(g.indexPrefixAttribute),
        this.algoliaClient = algoliasearch(this.clientId, this.apiKey),
        this.templates = b(h.RESULT_TEMPLATE_SELECTOR),
        this.fallbackTemplate = this.templates.filter(".default"),
        this.$defaultScope = b(h.DEFAULT_SCOPE_CLASS),
        this.$content = b(h.CONTENT_SELECTOR),
        this.$searchField = b(g.searchFieldCSSClass),
        this.$searchResults = b(h.CONTENT_SELECTOR + " " + h.SEARCH_RESULT_SELECTOR),
        this.$projectList = b(h.CONTENT_SELECTOR + " " + h.PROJECTS_LIST_SELECTOR))
    }
    ,
    f.init = function() {
        this.listenForChange(),
        this.listenForResultSelection(),
        this.clickOutside()
    }
    ,
    f.listenForResultSelection = function() {
        e.$defaultScope.on("click", ".search-results a", function(a) {
            k()
        })
    }
    ,
    f.clickOutside = function() {
        e.$defaultScope.on("click", ".container-fluid", function(a) {
            k()
        })
    }
    ,
    f.listenForChange = function() {
        e.$defaultScope.on("keyup", g.searchFieldCSSClass, function(a) {
            var c = b(this)
              , e = c.val();
            if ("" === e.trim())
                return void k(e);
            var f = []
              , j = "";
            if (g.attributesToSnippet.length > 0) {
                var l = g.attributesToSnippet.map(function(a) {
                    return a + ":" + h.SNIPPET_SIZE
                });
                j = l.join(",")
            }
            Object.keys(g.indices).forEach(function(a) {
                f.push({
                    indexName: p(a),
                    query: e,
                    params: {
                        attributesToSnippet: j,
                        hitsPerPage: h.RESULT_LIMIT
                    }
                })
            }),
            d.algoliaClient.search(f, i)
        })
    }
    ;
    var i = function(a, c) {
        if (null  !== a) {
            j();
            var d = o(h.ERROR_TEMPLATE, ["{{message}}"], a);
            return void b(h.CONTENT_SELECTOR + " .search-results").html(d)
        }
        var e = [];
        c.results.forEach(function(a) {
            var b = a.index;
            a.nbHits > 0 && e.push({
                index: b,
                header: g.indices[q(b)],
                results: a.hits
            })
        }),
        j(),
        e.forEach(l)
    }
      , j = function() {
        if (d.originalContainerContents)
            return void e.$searchResults.empty();
        e.$searchResults.removeClass(g.hiddenClass),
        e.$projectList.addClass(g.hiddenClass),
        e.$content.parent().addClass("search-results-container");
        var a = b('<span class="close-button">&times;</span>');
        a.on("click", function() {
            k()
        }),
        d.originalContainerContents = !0,
        e.$searchField.parent().prepend(a)
    }
      , k = function(a) {
        typeof a === c && (a = ""),
        e.$searchResults.addClass(g.hiddenClass),
        e.$projectList.removeClass(g.hiddenClass),
        d.originalContainerContents = !1,
        e.$searchField.val(a),
        e.$content.parent().removeClass("search-results-container"),
        e.$searchField.parent().find(".close-button").remove()
    }
      , l = function(a) {
        var c = a.index
          , e = a.header
          , f = a.results
          , g = d.templates.filter("." + q(c));
        0 === g.length && (g = d.fallbackTemplate);
        var i = g.html()
          , j = '<h1 class="heading-3">' + e + "</h1>"
          , k = i.match(/{{([^\}]+)(\}\})/g);
        f.forEach(function(a) {
            a = m(a);
            var b = o(i, k, a);
            j += b
        }),
        b(h.CONTENT_SELECTOR + " .search-results").append(j)
    }
      , m = function(a) {
        var b = ["_highlightResult", "_snippetResult"]
          , c = Object.keys(_.omit(a, b));
        return b.forEach(function(b) {
            a.hasOwnProperty(b) !== !1 && c.forEach(function(c) {
                var d = a[b];
                if (d.hasOwnProperty(c) !== !1) {
                    var d = a[b];
                    a[c] = d[c],
                    a[c] = n(a[c])
                }
            })
        }),
        a
    }
      , n = function r(a) {
        if (_.isObject(a) === !1)
            return a;
        if (a.hasOwnProperty("value"))
            return a = a.value;
        for (var b in a)
            a[b].hasOwnProperty("value") ? a[b] = a[b].value : a[b] = r(a[b]);
        return a
    }
      , o = function(a, b, c) {
        return b.forEach(function(b) {
            for (var d = b.replace(/[\{\}]/g, "").split("."), e = c, f = 0; f < d.length; f++) {
                var g = d[f];
                e.hasOwnProperty(g) && (null  === e[g] && (e[g] = ""),
                e = e[g])
            }
            a = a.replace(new RegExp(b,"gm"), e)
        }),
        a
    }
      , p = function(a) {
        return d.indexPrefix + "_" + a
    }
      , q = function(a) {
        return a.replace(d.indexPrefix + "_", "")
    }
    ;
    b(function() {
        b(g.searchFieldCSSClass).length && (d.init(),
        f.init())
    })
}(window, jQuery),
function(a, b, c) {
    var d, e, f, g, h, i;
    d = e = {},
    f = {},
    g = {},
    h = {},
    i = {
        FILTERABLE_CLASS: ".js-filterable",
        FILTER_CLASS: ".js-filterable-filter",
        CONTENT_CLASS: ".js-filterable-content",
        FILTERED_OUT_CLASS: "filtered-out",
        NO_RESULTS_MESSAGE: ".js-no-results",
        DEFAULT_SCOPE: ".js-default-content"
    },
    d.initializeFilterableFromElement = function(a) {
        return {
            $el: a,
            $input: a.find(i.FILTER_CLASS),
            $filterees: a.find(i.CONTENT_CLASS).children(),
            $noResultsMessage: a.find(i.NO_RESULTS_MESSAGE)
        }
    }
    ,
    f.filter = function(a, b) {
        var c, d = a.text().toLowerCase();
        return -1 === d.indexOf(b) ? (a.addClass(i.FILTERED_OUT_CLASS),
        c = !0) : (a.removeClass(i.FILTERED_OUT_CLASS),
        c = !1),
        c
    }
    ,
    f.hideNoResultsMessage = function(a) {
        a.$noResultsMessage.hide(),
        a.noResultsMessageDisplayed = !1
    }
    ,
    f.displayNoResultsMessage = function(a) {
        a.$noResultsMessage.fadeIn(),
        a.noResultsMessageDisplayed = !0
    }
    ,
    h.init = function(a) {
        a.find(i.FILTERABLE_CLASS).each(function() {
            var a = e.initializeFilterableFromElement(b(this));
            f.hideNoResultsMessage(a),
            h.listenToPageLoad(a),
            h.listenForKeyupEvent(a)
        })
    }
    ,
    h.listenToPageLoad = function(a) {
        a.$input.val() && this.onInputChange(a)
    }
    ,
    h.listenForKeyupEvent = function(a) {
        a.$el.on("keyup", function(b) {
            _.debounce(h.onInputChange.call(this, a), 50)
        })
    }
    ,
    h.onInputChange = function(a) {
        a.filtered = 0,
        a.$filterees.each(function() {
            var c = f.filter(b(this), a.$input.val().toLowerCase());
            c && a.filtered++
        }),
        a.noResultsMessageDisplayed || a.$filterees.length !== a.filtered ? a.noResultsMessageDisplayed && a.$filterees.length !== a.filtered && f.hideNoResultsMessage(a) : f.displayNoResultsMessage(a);
    }
    ;
    var j = function(a) {
        h.init(a)
    }
    ;
    b(function() {
        Crew.Utils.initializers.add(function(a) {
            j(a)
        }),
        j(b(i.DEFAULT_SCOPE))
    })
}(window, jQuery),
function(a, b, c) {
    var d = {
        SELECTABLES_SCOPE: ".js-list-selected-scope",
        DEFAULT_SCOPE: ".js-default-content"
    }
      , e = function(a) {
        var c, d, e, f, g, h;
        c = d = {},
        e = {},
        f = {},
        g = {},
        h = {
            DEST_CLASS: ".js-list-selected",
            SELECTED_TEXT_CLASS: ".js-list-selected-message",
            SELECTED_ITEM_DEFAULT_HTML: "<span>{{item}}</span>"
        },
        c.init = function() {
            this.$destination = a.find(h.DEST_CLASS),
            this.$checkboxes = b(this.$destination.data("selectables")),
            this.valueAttribute = this.$destination.data("value"),
            this.selectedItemFormat = this.$destination.data("item-format"),
            this.$selectedMessage = a.find(h.SELECTED_TEXT_CLASS)
        }
        ,
        f.init = function() {
            this.selected = []
        }
        ,
        f.populateSelectedArray = function(a) {
            var b = d.valueAttribute ? a.data(d.valueAttribute) : a.val();
            if (a.is(":checked"))
                this.selected.push(b);
            else {
                var c = this.selected.indexOf(b);
                this.selected.splice(c, 1)
            }
        }
        ,
        e.render = function() {
            var a = [];
            b.each(f.selected, function(b, c) {
                a.push(e.getHtmlTag(c))
            }),
            e.showHideSelectedMessage(a),
            d.$destination.html(a.join(""))
        }
        ,
        e.getHtmlTag = function(a) {
            var b = h.SELECTED_ITEM_DEFAULT_HTML;
            return d.selectedItemFormat && (b = d.selectedItemFormat),
            b.replace("{{item}}", a)
        }
        ,
        e.showHideSelectedMessage = function(a) {
            a.length > 0 ? d.$selectedMessage.show() : d.$selectedMessage.hide()
        }
        ,
        g.init = function() {
            this.listenToPageLoad(),
            this.listenToCheckboxChange()
        }
        ,
        g.listenToPageLoad = function() {
            var a = d.$checkboxes.filter(":checked");
            _.each(a, function(a) {
                f.populateSelectedArray(b(a))
            }),
            e.render()
        }
        ,
        g.listenToCheckboxChange = function() {
            d.$checkboxes.on("change", function(a) {
                f.populateSelectedArray(b(this)),
                e.render()
            })
        }
        ,
        c.init(),
        f.init(),
        g.init()
    }
      , f = function(a) {
        a.find(d.SELECTABLES_SCOPE).each(function() {
            e(b(this))
        })
    }
    ;
    b(function() {
        Crew.Utils.initializers.add(function(a) {
            f(a)
        }),
        f(b(d.DEFAULT_SCOPE))
    })
}(window, jQuery);
var Crew = Crew || {};
!function(a, b, c) {
    var d, e, f, g;
    e = d = {},
    f = {},
    g = {
        DEFAULT_SCOPE: ".js-default-content",
        TRIGGER_CLASS: ".js-auto-size"
    },
    e.init = function() {
        this.$defaultScope = b(g.DEFAULT_SCOPE)
    }
    ,
    f.init = function(a) {
        var b;
        a = a || d.$defaultScope,
        b = a.find("textarea.js-auto-size"),
        b.length && autosize(b)
    }
    ,
    b(function() {
        e.init(),
        f.init(),
        Crew.Utils.initializers.add(function(a) {
            f.init(a)
        })
    })
}(window, jQuery);
var Crew = Crew || {};
!function(a, b, c) {
    var d, e, f, g;
    e = d = {},
    f = {},
    g = {
        DEFAULT_SCOPE: ".js-default-content",
        TRIGGER_CLASS: ".js-check-all",
        TRIGGER_DATA_ATTR: "targetClass"
    },
    e.init = function() {
        this.$defaultScope = b(g.DEFAULT_SCOPE)
    }
    ,
    f.init = function(a) {
        a = a || d.$defaultScope;
        var c = a.find(g.TRIGGER_CLASS);
        c.length && c.on("click", function() {
            var a = b(this);
            f.updateAll(b(a.data(g.TRIGGER_DATA_ATTR)), a[0].checked)
        })
    }
    ,
    f.updateAll = function(a, b) {
        a.prop("checked", b)
    }
    ,
    b(function() {
        e.init(),
        f.init(),
        Crew.Utils.initializers.add(function(a) {
            f.init(a)
        })
    })
}(window, jQuery),
function(a, b, c) {
    var d, e, f, g, h;
    g = {},
    h = {},
    d = {},
    e = {
        elementSelector: "[data-select-toggle]",
        scopeAttribute: "data-select-toggle-scope",
        panelSelector: "[data-select-toggle-panel]"
    },
    f = {
        DEFAULT_SCOPE: ".js-default-content"
    },
    g.getFromScope = function(a) {
        this.$select = a.find(e.elementSelector);
        var b = this.$select.attr(e.scopeAttribute);
        this.$scope = a.find(b)
    }
    ,
    h.toggle = function(a) {
        g.getFromScope(a);
        var b = g.$select.val();
        g.$scope.find(e.panelSelector).hide(),
        g.$scope.find(e.panelSelector).filter("." + b).show()
    }
    ,
    d.init = function(a) {
        this.listenToChangeEvent(a),
        h.toggle(a)
    }
    ,
    d.listenToChangeEvent = function(a) {
        g.getFromScope(a),
        g.$select.on("change", function() {
            h.toggle(a)
        })
    }
    ;
    var i = function(a) {
        d.init(a)
    }
    ;
    b(function() {
        Crew.Utils.initializers.add(function(a) {
            i(a)
        }),
        i(b(f.DEFAULT_SCOPE))
    })
}(window, jQuery),
function(a, b, c) {
    var d = {
        NETWORK_ID_RADIO_BUTTON: "input[name=network_id]:radio",
        NETWORK_DEV_CONTAINER_JS_CLASS: ".js-list-network-devs",
        NETWORK_DEV_RADIO_BUTTON: ".js-network-devs-radio",
        NETWORK_DEV_CHECKBOX_JS_CLASS: ".js-suggest-dev-id",
        DEFAULT_SCOPE: ".js-default-content"
    }
      , e = function(c) {
        var e, f, g, h, i;
        e = f = {},
        g = {},
        h = {},
        i = {},
        e.init = function() {
            this.$scope = b(c),
            this.$networkDevsContainer = this.$scope.find(d.NETWORK_DEV_CONTAINER_JS_CLASS),
            this.$networkIdRadios = this.$scope.find(d.NETWORK_ID_RADIO_BUTTON),
            this.$networkDevRadio = this.$scope.find(d.NETWORK_DEV_RADIO_BUTTON),
            this.$networkDevCheckboxes = this.$scope.find(d.NETWORK_DEV_CHECKBOX_JS_CLASS),
            this.$submitButton = this.$networkIdRadios.parents("form").find("input[type=submit]")
        }
        ,
        h.init = function() {
            this.selectedCheckboxes = [],
            this.devContainerOpen = !1
        }
        ,
        g.init = function() {
            this.replaceInputValues(),
            this.showHideContainer(f.$networkIdRadios.filter(":checked"))
        }
        ,
        g.showHideContainer = function(a) {
            a.is(f.$networkDevRadio) ? (f.$networkDevsContainer.show(),
            g.selectCheckboxes(),
            h.devContainerOpen = !0) : (f.$networkDevsContainer.hide(),
            g.unselectCheckboxes(),
            h.devContainerOpen = !1)
        }
        ,
        g.replaceInputValues = function() {
            b.each(f.$networkIdRadios, function(a, c) {
                var d = b(this)
                  , e = d.data("input-value");
                e && d.val(e)
            })
        }
        ,
        g.unselectCheckboxes = function() {
            h.selectedCheckboxes = f.$networkDevCheckboxes.filter(":checked"),
            f.$networkDevCheckboxes.prop("checked", !1)
        }
        ,
        g.selectCheckboxes = function() {
            h.selectedCheckboxes.length <= 0 || b(h.selectedCheckboxes).prop("checked", !0)
        }
        ,
        g.enableOrDisableSubmit = function() {
            if (h.devContainerOpen) {
                var a = f.$networkDevCheckboxes.filter(":checked");
                f.$submitButton.prop("disabled", a.length <= 0)
            } else
                f.$submitButton.prop("disabled", !1)
        }
        ,
        i.init = function() {
            this.listenToRadios(),
            this.listenToCheckboxes(),
            this.listenToInputs()
        }
        ,
        i.listenToInputs = function() {
            f.$scope.find(".js-ajax-field:not(.js-ajax-field-ignore)").on("keyup change", function(b) {
                a.setTimeout(g.enableOrDisableSubmit, 5)
            })
        }
        ,
        i.listenToRadios = function() {
            f.$networkIdRadios.on("change", function(a) {
                g.showHideContainer(b(this)),
                g.enableOrDisableSubmit()
            })
        }
        ,
        i.listenToCheckboxes = function() {
            f.$networkDevCheckboxes.on("change", function(a) {
                g.enableOrDisableSubmit()
            })
        }
        ,
        e.init(),
        f.$networkDevsContainer.length <= 0 || (h.init(),
        i.init(),
        g.init())
    }
      , f = function(a) {
        e(a)
    }
    ;
    b(function() {
        Crew.Utils.initializers.add(function(a) {
            f(a)
        }),
        f(b(d.DEFAULT_SCOPE))
    })
}(window, jQuery),
function(a, b, c) {
    var d = {
        DEFAULT_SCOPE: ".js-default-content",
        REFERRER_FEE_INPUT_JS_CLASS: ".js-referrer-fee-input",
        TOTAL_FEE_OUTPUT_JS_CLASS: ".js-total-fee-output",
        CREW_FEE_ATTRIBUTE: "data-network-fee-calculator-crew-fee",
        MAX_COMMISSION_ATTRIBUTE: "data-network-fee-calculator-max-commission"
    }
      , e = function(a) {
        var c, e, f, g;
        c = e = {},
        f = {},
        g = {},
        c.init = function() {
            this.$input = a.find(d.REFERRER_FEE_INPUT_JS_CLASS),
            this.$output = a.find(d.TOTAL_FEE_OUTPUT_JS_CLASS),
            this.crewFee = Number(this.$input.attr(d.CREW_FEE_ATTRIBUTE)),
            this.maxCommission = Number(this.$input.attr(d.MAX_COMMISSION_ATTRIBUTE))
        }
        ,
        f.init = function() {
            this.getTotalFee(e.$input.val())
        }
        ,
        f.getTotalFee = function(a) {
            var b = parseFloat(c.crewFee)
              , d = "success";
            return "" === a ? this.displayFee(b, d) : (a = parseInt(a),
            isNaN(a) || 0 > a ? this.displayFee(0, "danger") : (b += a,
            a > c.maxCommission && (d = "danger"),
            this.displayFee(b, d)))
        }
        ,
        f.displayFee = function(a, b) {
            var c = '<span class="text-' + b + '">' + a.toFixed(0) + "%</span>";
            e.$output.html(c)
        }
        ,
        g.init = function() {
            this.listenToInputChange()
        }
        ,
        g.listenToInputChange = function() {
            e.$input.on("keyup", function(a) {
                f.getTotalFee(b(this).val())
            })
        }
        ,
        b(function() {
            c.init(),
            0 === c.$input.length || isNaN(c.crewFee) || isNaN(c.maxCommission) || (g.init(),
            f.init())
        })
    }
    ;
    b(function() {
        Crew.Utils.initializers.add(function(a) {
            e(a)
        }),
        e(b(d.DEFAULT_SCOPE))
    })
}(window, jQuery),
function(a, b, c) {
    var d, e, f, g, h;
    e = d = {},
    f = {},
    g = {},
    h = {
        MENU_TOGGLE: ".menu-toggle",
        CONTAINER_FLUID: ".container-fluid",
        CONTAINER_LEFT_SIDEBAR: ".container-left.sidebar",
        EXPANDED_CLASS: "expanded"
    },
    e.init = function() {
        this.$menuToggle = b(h.MENU_TOGGLE),
        this.$containerFluid = b(h.CONTAINER_FLUID),
        this.$containerLeftSidebar = b(h.CONTAINER_LEFT_SIDEBAR)
    }
    ,
    f.init = function() {
        this.visible = !1
    }
    ,
    g.init = function() {
        d.$menuToggle.on("click", g.toggle)
    }
    ,
    g.toggle = function() {
        var a;
        f.visible = !f.visible,
        a = f.visible ? "on" : "off",
        d.$containerLeftSidebar.toggleClass(h.EXPANDED_CLASS),
        d.$containerFluid[a]("click", g.toggle),
        d.$containerLeftSidebar[a]("click", "a", g.toggle)
    }
    ,
    b(function() {
        e.init(),
        f.init(),
        g.init()
    })
}(window, jQuery);
var Crew = Crew || {};
Crew.Mobile = Crew.Mobile || {},
function(a, b, c) {
    var d, e, f, g, h, i;
    e = d = {},
    f = {},
    g = {},
    h = {},
    i = {
        DEFAULT_CONTENT: ".js-default-content",
        CONTAINER_MIDDLE: ".container-middle",
        CONTAINER_RIGHT: ".container-right",
        COLUMNS_RESET: ".js-mobile-toggle-reset",
        HIDDEN_CLASS: "mobile-hidden",
        VISIBLE_CLASS: "mobile-visible"
    },
    e.init = function() {
        e.$defaultContent = b(i.DEFAULT_CONTENT)
    }
    ,
    f.init = function() {
        d.$defaultContent.on("click", ".js-mobile-toggle", function(a) {
            a.preventDefault(),
            b(i.CONTAINER_MIDDLE).addClass(i.HIDDEN_CLASS),
            b(i.CONTAINER_RIGHT).addClass(i.VISIBLE_CLASS),
            b(i.COLUMNS_RESET).show()
        }),
        d.$defaultContent.on("click", i.COLUMNS_RESET, function(a) {
            a.preventDefault(),
            f.resetColumnsVisibility()
        })
    }
    ,
    f.resetColumnsVisibility = function() {
        b(i.CONTAINER_MIDDLE).removeClass(i.HIDDEN_CLASS),
        b(i.CONTAINER_RIGHT).removeClass(i.VISIBLE_CLASS),
        b(i.COLUMNS_RESET).hide()
    }
    ,
    b(function() {
        e.init(),
        f.init()
    }),
    b.extend(Crew.Mobile, {
        resetColumnsVisibility: f.resetColumnsVisibility
    })
}(window, jQuery),
function(a, b, c) {
    var d, e;
    d = {},
    e = {
        PREFIX_SELECTOR: "[data-header-title-prefix]"
    },
    d.init = function() {
        b(".js-default-content").on("click", e.PREFIX_SELECTOR, function() {
            var a = Crew.Layout.getRegion("header");
            a && a.setTitlePrefix(b(this).data("headerTitlePrefix"))
        })
    }
    ,
    b(function() {
        d.init()
    })
}(window, jQuery),
function(a, b, c) {
    var d;
    d = {},
    d.init = function(a) {
        var b = a.find(".js-limit-characters");
        b.length && (this.addInputLimitNotices(b),
        this.listenForInputLimitTyping(b))
    }
    ,
    d.addInputLimitNotices = function(a) {
        a.each(function() {
            var a, d, e;
            a = b(this),
            d = a.attr("maxlength"),
            d !== c && (e = 1 == d ? "character" : "characters",
            b('<span class="js-limit-notice help-block sm-margin-null text-right"><span class="remaining">' + d + '</span> <span class="item">' + e + "</span> remaining</span>").insertAfter(a))
        })
    }
    ,
    d.listenForInputLimitTyping = function(a) {
        a.on("keyup", function(a) {
            var c = b(this)
              , d = c.attr("maxlength") - c.val().length
              , e = 1 == d ? "character" : "characters"
              , f = c.siblings(".js-limit-notice");
            b(".remaining", f).text(d),
            b(".item", f).text(e),
            10 > d ? f.addClass("text-danger") : f.removeClass("text-danger")
        })
    }
    ,
    b(function() {
        d.init(b(".js-default-content")),
        Crew.Utils.initializers.add(function(a) {
            d.init(a)
        })
    })
}(window, jQuery);
var Crew = Crew || {};
Crew.Stripe = Crew.Stripe || {},
function(a, b, c) {
    var d, e, f, g, h, i;
    d = e = {},
    f = {},
    g = {},
    h = {},
    i = {
        PAYMENT_FORM_JS_CLASS: ".js-stripe-checkout",
        PAY_BUTTON_JS_CLASS: ".js-stripe-pay",
        RESPONSE_MESSAGE_JS_CLASS: ".js-stripe-response-message",
        PROGRESS_MESSAGE_JS_CLASS: ".js-stripe-progress",
        events: {
            READY: "ready.CrewStripe",
            PAID: "paid.CrewStripe",
            POPSTATE: "popstate.CrewStripe",
            SUBMIT: "submit.CrewStripe"
        }
    },
    d.init = function(c) {
        this.$window = b(a),
        this.$paymentForm = this.$findPaymentForm(c),
        this.$paymentButton = this.$paymentForm.find(i.PAY_BUTTON_JS_CLASS),
        this.$paymentResponseMessage = this.$paymentForm.find(i.RESPONSE_MESSAGE_JS_CLASS),
        this.$progressMessage = this.$paymentForm.find(i.PROGRESS_MESSAGE_JS_CLASS),
        this.$stripeFields = this.$paymentForm.find("input[type=hidden][data-stripe-field=true]"),
        this.stripeFieldNames = _.map(this.$stripeFields, function(a) {
            return a.name
        })
    }
    ,
    d.$findPaymentForm = function(a) {
        return a.find(i.PAYMENT_FORM_JS_CLASS)
    }
    ,
    f.changePayButtonDisabledStateTo = function(a) {
        e.$paymentButton.attr("disabled", a)
    }
    ,
    f.getAttributesFromForm = function() {
        return {
            image: e.$paymentForm.data("image")
        }
    }
    ,
    f.showResponseMessage = function(a, b) {
        e.$paymentResponseMessage.removeClass().addClass("text-" + b).html(a)
    }
    ,
    f.hideResponseMessage = function() {
        e.$paymentResponseMessage.empty()
    }
    ,
    f.toggleProgress = function(a) {
        a ? e.$progressMessage.show() : e.$progressMessage.hide()
    }
    ,
    f.setStripeData = function() {
        g.config = f.getAttributesFromForm();
        var a = {};
        _.each(e.$stripeFields, function(c) {
            var d = b(c);
            a[d.attr("name")] = d.val()
        }),
        b.extend(g.config, a)
    }
    ,
    g.init = function() {
        this.requestData = {},
        this.config = {},
        this.initStripe(),
        f.changePayButtonDisabledStateTo(!1)
    }
    ,
    g.hasForm = function(a) {
        return e.$findPaymentForm(a).length > 0
    }
    ,
    g.getRequestData = function(a) {
        var c = {}
          , d = a.serializeArray();
        return _.each(d, function(a) {
            -1 === b.inArray(a.name, e.stripeFieldNames) && (c[a.name] = a.value)
        }),
        c
    }
    ,
    g.initStripe = function() {
        if ("undefined" == typeof StripeCheckout)
            throw new Error("You need to load the Stripe checkout.js script first..");
        if (!this.Stripe) {
            var a = this;
            this.Stripe = StripeCheckout.configure({
                name: "Crew",
                key: CrewStripeApiKey,
                billingAddress: !0,
                currency: "USD",
                token: function(b) {
                    return a.pay(b)
                }
            })
        }
    }
    ,
    g.initPayment = function() {
        f.setStripeData(),
        this.Stripe.open(g.config)
    }
    ,
    g.pay = function(a) {
        var c = b("body").first().data("csrfToken")
          , d = {
            stripeToken: a.id
        };
        b.extend(this.requestData, d),
        b.ajax({
            url: e.$paymentForm.attr("action"),
            type: "POST",
            headers: {
                "X-CSRF-Token": c
            },
            data: this.requestData,
            dataType: "json",
            success: this.handleResponse,
            error: this.handleError,
            beforeSend: function() {
                f.hideResponseMessage(),
                f.toggleProgress(!0),
                f.changePayButtonDisabledStateTo(!0)
            }
        })
    }
    ,
    g.handleError = function(a, b, c) {
        var d = "Something went wrong. Contact support.";
        a.responseJSON && a.responseJSON.message && (d = a.responseJSON.message),
        f.toggleProgress(!1),
        f.showResponseMessage(d, "danger")
    }
    ,
    g.handleResponse = function(a, b, c) {
        var d = a.isSuccess ? "success" : "danger";
        f.showResponseMessage(a.data, d),
        f.toggleProgress(!1),
        a.isSuccess ? setTimeout(function() {
            g.updateRegions()
        }, 200) : f.changePayButtonDisabledStateTo(!1)
    }
    ,
    g.updateRegions = function() {
        Crew.Layout.getRegion("right").update(),
        Crew.Layout.getRegion("middle").update()
    }
    ,
    h.initAllTheThings = function(a) {
        g.hasForm(a) !== !1 && (d.init(a),
        g.init(),
        h.init())
    }
    ,
    h.killAllTheThings = function(a) {
        g.hasForm(a) !== !1 && h.reset()
    }
    ,
    h.init = function() {
        this.listenToPayAction(),
        this.listenToPageUnload()
    }
    ,
    h.reset = function() {
        e.$window.off(i.events.POPSTATE),
        e.$paymentForm.off(i.events.SUBMIT)
    }
    ,
    h.listenToPageUnload = function() {
        e.$window.on(i.events.POPSTATE, function(a) {
            g.Stripe.close()
        })
    }
    ,
    h.listenToPayAction = function() {
        e.$paymentForm.off(i.events.SUBMIT).on(i.events.SUBMIT, function(a) {
            a.preventDefault(),
            f.hideResponseMessage(),
            g.requestData = g.getRequestData(b(this)),
            g.initPayment()
        })
    }
    ,
    b(function() {
        h.initAllTheThings(b(document))
    }),
    Crew.Utils.initializers.add(function(a) {
        h.initAllTheThings(a)
    }),
    Crew.Utils.terminators.add(function(a) {
        h.killAllTheThings(a)
    }),
    b.extend(a.Crew.Stripe, {
        constants: i,
        getPaymentForm: d.$findPaymentForm,
        hasForm: g.hasForm,
        enableStripeEvent: h.listenToPayAction,
        getRequestData: g.getRequestData,
        changePayButtonDisabledStateTo: f.changePayButtonDisabledStateTo
    })
}(window, jQuery);
var Crew = Crew || {};
Crew.Stripe = Crew.Stripe || {},
Crew.Utils = Crew.Utils || {},
Crew.Utils.Money = Crew.Utils.Money || {},
function(a, b, c) {
    var d, e, f, g, h, i, j;
    d = e = {},
    f = {},
    g = {},
    h = {},
    i = b.extend(!0, {
        AMOUNT_FIELD_JS_CLASS: ".js-stripe-checkout-amount",
        AMOUNT_MODIFIER_JS_CLASS: ".js-stripe-amount-modifier",
        events: {
            AMOUNT_UPDATED: "amountUpdated.CrewStripe.modifier",
            KEYUP: "keyup.CrewStripe.modifier",
            CHANGE: "change.CrewStripe.modifier"
        }
    }, Crew.Stripe.constants),
    j = {
        operators: {
            "+": function(a, b) {
                return a + b
            },
            "-": function(a, b) {
                return a - b
            }
        },
        oppositeOperators: {
            "+": "-",
            "-": "+"
        },
        eventTypes: {
            text: i.events.KEYUP,
            checkbox: i.events.CHANGE
        }
    },
    d.init = function(a) {
        this.$paymentForm = Crew.Stripe.getPaymentForm(a),
        this.$amountModifierFields = this.$findModifiers(a),
        this.$paymentButton = this.$paymentForm.find(i.PAY_BUTTON_JS_CLASS),
        this.$amountField = this.$paymentForm.find(i.AMOUNT_FIELD_JS_CLASS)
    }
    ,
    d.$findModifiers = function(a) {
        return a.find(i.AMOUNT_MODIFIER_JS_CLASS)
    }
    ,
    f.init = function() {
        this.setIdToInputs()
    }
    ,
    f.setIdToInputs = function() {
        _.each(e.$amountModifierFields, function(a) {
            b(a).attr("data-stripe-checkout-id", _.uniqueId())
        })
    }
    ,
    f.updateAmount = function(a) {
        Crew.Stripe.changePayButtonDisabledStateTo(!1),
        0 >= a && (a = 0,
        Crew.Stripe.changePayButtonDisabledStateTo(!0)),
        e.$amountField.val(a)
    }
    ,
    f.updateSubmitLabel = function(a, b) {
        var c = "Fund $" + Crew.Utils.Money.formatCurrency(a / 100);
        b > 0 && (c += " (+ $" + Crew.Utils.Money.formatCurrency(b / 100) + ")"),
        0 >= a && (c = "Cannot fund $0.00"),
        e.$paymentButton.val(c)
    }
    ,
    f.calculateAmount = function(a, b) {
        var c = j.operators[b](g.totalAmount, a);
        return g.totalAmount = c,
        c
    }
    ,
    f.getAmountForCheckbox = function(a) {
        var b = f.getModifiers(a);
        return a.is(":checked") || (b.operator = j.oppositeOperators[b.operator]),
        this.calculateAmount(b.amount, b.operator)
    }
    ,
    f.getAmountForText = function(a) {
        var b = f.getModifiers(a)
          , c = a.data("stripe-checkout-id")
          , d = g.amountHistory[c][g.amountHistory[c].length - 1];
        return g.totalAmount = this.calculateAmount(d, j.oppositeOperators[b.operator]),
        g.amountHistory[c].push(b.amount),
        this.calculateAmount(b.amount, b.operator)
    }
    ,
    f.getModifiers = function(a) {
        var b = a.data("amount-modifier")
          , c = a.data("amount-modifier-operator") || "+"
          , d = a.data("amount-modifier-multiplier") || 1;
        if (!(c in j.operators))
            throw new Error("Operator modifier logic hasn't been defined.");
        if ("value" === b) {
            var e = parseFloat(a.val());
            return e = isNaN(e) ? 0 : e * d,
            {
                amount: e,
                operator: c
            }
        }
        var f = parseFloat(b);
        if (isNaN(f))
            throw new Error("Amount modifier is not properly formatted or not defined.");
        return {
            amount: f * d,
            operator: c
        }
    }
    ,
    g.init = function() {
        this.totalAmount = parseFloat(e.$amountField.val()),
        this.generateInputValuesHistory()
    }
    ,
    g.getDelegatedMethod = function(a) {
        var b = a.attr("type")
          , c = b.charAt(0).toUpperCase() + b.substr(1);
        return "getAmountFor" + c
    }
    ,
    g.generateInputValuesHistory = function() {
        var a = this;
        this.amountHistory = [],
        _.each(e.$amountModifierFields, function(c) {
            var d = b(c)
              , e = d.data("amount-modifier-multiplier") || 1;
            a.amountHistory[d.data("stripe-checkout-id")] = [d.val() * e]
        })
    }
    ,
    g.hasModifiers = function(a) {
        return e.$findModifiers(a).length > 0
    }
    ,
    h.initAllTheThings = function(a) {
        Crew.Stripe.hasForm(a) !== !1 && (d.init(a),
        g.hasModifiers(a) !== !1 && (f.init(),
        g.init(),
        h.init()))
    }
    ,
    h.init = function() {
        this.listenToAmountModifiers()
    }
    ,
    h.reset = function() {
        _.each(j.eventTypes, function(a) {
            e.$amountModifierFields.off(a)
        })
    }
    ,
    h.listenToAmountModifiers = function() {
        var a = this;
        _.each(e.$amountModifierFields, function(c) {
            a.bindAppropriateEvent(b(c))
        })
    }
    ,
    h.bindAppropriateEvent = function(a) {
        var c = j.eventTypes[a.attr("type")];
        a.on(c, function(a) {
            var c = b(this)
              , d = g.getDelegatedMethod(c)
              , h = parseFloat(f[d](c));
            f.updateAmount(h),
            f.updateSubmitLabel(h, 0),
            e.$amountField.triggerHandler(i.events.AMOUNT_UPDATED)
        })
    }
    ,
    b(function() {
        h.initAllTheThings(b(document))
    }),
    Crew.Utils.initializers.add(function(a) {
        h.initAllTheThings(a)
    }),
    Crew.Utils.terminators.add(function(a) {
        Crew.Stripe.hasForm(a) !== !1 && g.hasModifiers(a) !== !1 && h.reset()
    }),
    b.extend(a.Crew.Stripe, {
        constants: i,
        updateSubmitLabel: f.updateSubmitLabel,
        updateAmount: f.updateAmount
    })
}(window, jQuery);
var Crew = Crew || {};
Crew.Stripe = Crew.Stripe || {},
function(a, b, c) {
    var d, e, f, g, h, i;
    d = e = {},
    f = {},
    g = {},
    h = {},
    i = b.extend(!0, {
        FEE_FIELD_JS_CLASS: ".js-stripe-fee",
        events: {
            FEE_CHANGE: "change.CrewStripe.fee"
        }
    }, Crew.Stripe.constants),
    d.init = function(a) {
        this.$paymentForm = Crew.Stripe.getPaymentForm(a),
        this.$feeFields = this.$findFeeFields(a),
        this.$amountField = this.$paymentForm.find(i.AMOUNT_FIELD_JS_CLASS)
    }
    ,
    d.$findFeeFields = function(a) {
        return a.find(i.FEE_FIELD_JS_CLASS)
    }
    ,
    f.applyFee = function() {
        var a = g.currentFeePercentage;
        if (isNaN(a))
            throw new Error("Fee must be a float/int");
        var b = g.amountWithoutFee * (1 + a)
          , c = b - g.amountWithoutFee;
        Crew.Stripe.updateAmount(b),
        Crew.Stripe.updateSubmitLabel(g.amountWithoutFee, c)
    }
    ,
    f.updateFee = function(a) {
        g.currentFeePercentage = a.data("fee"),
        f.applyFee()
    }
    ,
    g.init = function() {
        this.amountWithoutFee = parseFloat(e.$amountField.val()),
        this.applyFeeAtLoading()
    }
    ,
    g.hasFeeFields = function(a) {
        return e.$findFeeFields(a).length > 0
    }
    ,
    g.applyFeeAtLoading = function() {
        var a = e.$feeFields.filter(":checked");
        this.currentFeePercentage = 0,
        a.length > 0 && (this.currentFeePercentage = a.data("fee")),
        f.applyFee()
    }
    ,
    h.initAllTheThings = function(a) {
        Crew.Stripe.hasForm(a) !== !1 && g.hasFeeFields(a) !== !1 && (d.init(a),
        g.init(),
        h.init())
    }
    ,
    h.killThemAll = function(a) {
        Crew.Stripe.hasForm(a) !== !1 && g.hasFeeFields(a) !== !1 && h.reset()
    }
    ,
    h.init = function() {
        this.listenToAmountChange(),
        this.listenToFeeChange()
    }
    ,
    h.reset = function() {
        e.$amountField.off(i.events.AMOUNT_UPDATED),
        e.$feeFields.off(i.events.FEE_CHANGE)
    }
    ,
    h.listenToAmountChange = function() {
        e.$amountField.on(i.events.AMOUNT_UPDATED, function(a) {
            g.amountWithoutFee = parseFloat(b(this).val()),
            f.applyFee()
        })
    }
    ,
    h.listenToFeeChange = function() {
        e.$feeFields.on(i.events.FEE_CHANGE, function(a) {
            f.updateFee(b(this))
        })
    }
    ,
    b(function() {
        h.initAllTheThings(b(document))
    }),
    Crew.Utils.initializers.add(function(a) {
        h.initAllTheThings(a)
    }),
    Crew.Utils.terminators.add(function(a) {
        h.killThemAll(a)
    }),
    b.extend(a.Crew.Stripe, {
        constants: i
    })
}(window, jQuery);
var Crew = Crew || {};
Crew.Stripe = Crew.Stripe || {},
Crew.Ajax = Crew.Ajax || {},
Crew.Ajax.Form = Crew.Ajax.Form || {},
Crew.Utils = Crew.Utils || {},
Crew.Utils.Money = Crew.Utils.Money || {},
function(a, b, c) {
    var d, e, f, g, h, i;
    d = e = {},
    f = {},
    g = {},
    h = {},
    i = b.extend(!0, {
        BANK_WIRE_BUTTON_JS_CLASS: ".js-bank-wire",
        BANK_WIRE_MODAL_JS_CLASS: ".js-bank-wire-modal",
        BANK_WIRE_AMOUNT_JS_CLASS: ".js-bank-wire-amount",
        BANK_WIRE_HIDDEN_FIELD_JS_CLASS: ".js-bank-wire-hidden",
        BANK_WIRE_CONFIRMATION_JS_CLASS: ".js-bank-wire-confirmation",
        events: {
            BANK_WIRE_SELECTION: "change.CrewStripe.bankWire"
        }
    }, Crew.Stripe.constants),
    d.init = function(a) {
        this.$bankWireButton = a.find(i.BANK_WIRE_BUTTON_JS_CLASS),
        this.$bankWireModal = a.find(i.BANK_WIRE_MODAL_JS_CLASS),
        this.$bankWireAmount = this.$bankWireModal.find(i.BANK_WIRE_AMOUNT_JS_CLASS),
        this.$bankWireForm = this.$bankWireModal.find("form"),
        this.$bankWireConfirmation = this.$bankWireModal.find(i.BANK_WIRE_CONFIRMATION_JS_CLASS),
        this.$bankWireDismiss = this.$bankWireModal.find("[data-dismiss=modal]"),
        this.$paymentForm = Crew.Stripe.getPaymentForm(a),
        this.$paymentMethods = this.$paymentForm.find(i.FEE_FIELD_JS_CLASS),
        this.$amountField = this.$paymentForm.find(i.AMOUNT_FIELD_JS_CLASS)
    }
    ,
    f.displayAmount = function() {
        var a = parseFloat(e.$amountField.val()) / 100
          , b = Crew.Utils.Money.formatCurrency(a);
        e.$bankWireAmount.html("$" + b)
    }
    ,
    f.createHiddenFields = function(a) {
        var b = Crew.Stripe.getRequestData(a);
        _.each(b, function(a, b) {
            var c = '<input type="hidden" name="' + b + '" value="' + a + '" class="js-bank-wire-hidden">';
            e.$bankWireForm.append(c)
        })
    }
    ,
    f.showConfirmation = function() {
        e.$bankWireForm.fadeOut(function() {
            e.$bankWireConfirmation.show(),
            e.$bankWireDismiss.attr("disabled", !1)
        })
    }
    ,
    g.markAsSubmitted = function() {
        g.hasBeenSubmitted = !0
    }
    ,
    h.initAllTheThings = function(a) {
        Crew.Stripe.hasForm(a) !== !1 && (d.init(a),
        h.init())
    }
    ,
    h.init = function() {
        this.listenToBankWireSelection(),
        this.listenToModalCloseEvent(),
        this.listenToFormSubmit()
    }
    ,
    h.reset = function() {
        e.$paymentMethods.off(i.events.BANK_WIRE_SELECTION),
        g.hasBeenSubmitted = c
    }
    ,
    h.listenToBankWireSelection = function() {
        e.$paymentMethods.on(i.events.BANK_WIRE_SELECTION, function(a) {
            b(this).is(e.$bankWireButton) ? h.enableBankWireEvent() : Crew.Stripe.enableStripeEvent()
        })
    }
    ,
    h.listenToModalCloseEvent = function() {
        e.$bankWireModal.on("hidden.bs.modal", function(a) {
            Crew.Ajax.Form.reset(e.$bankWireForm),
            Crew.Ajax.Form.enableSaveButton(e.$bankWireForm),
            e.$bankWireForm.find(i.BANK_WIRE_HIDDEN_FIELD_JS_CLASS).remove(),
            g.hasBeenSubmitted && (Crew.Layout.getRegion("middle").update(),
            Crew.Layout.getRegion("right").update(),
            g.hasBeenSubmitted = c)
        })
    }
    ,
    h.enableBankWireEvent = function() {
        e.$paymentForm.off(i.events.SUBMIT).on(i.events.SUBMIT, function(a) {
            a.preventDefault(),
            e.$bankWireModal.modal("show"),
            f.displayAmount(),
            f.createHiddenFields(b(this))
        })
    }
    ,
    h.listenToFormSubmit = function() {
        e.$bankWireForm.off("submit").on("submit", function(a) {
            var c = b(this);
            a.stopPropagation(),
            a.preventDefault(),
            e.$bankWireDismiss.attr("disabled", !0),
            Crew.Ajax.Form.submit(c, h.handleSubmission)
        })
    }
    ,
    h.handleSubmission = function() {
        g.markAsSubmitted(),
        setTimeout(f.showConfirmation, 700)
    }
    ,
    b(function() {
        h.initAllTheThings(b(document))
    }),
    Crew.Utils.initializers.add(function(a) {
        h.initAllTheThings(a)
    }),
    Crew.Utils.terminators.add(function(a) {
        Crew.Stripe.hasForm(a) !== !1 && h.reset()
    })
}(window, jQuery),
$(".js-welcome-modal").modal({
    backdrop: "static"
});
var Crew = Crew || {};
Crew.Utils = Crew.Utils || {},
Crew.Ajax = Crew.Ajax || {},
Crew.Ajax.Form = Crew.Ajax.Form || {},
function(a, b, c) {
    var d, e, f, g, h, i;
    d = e = {},
    f = {},
    g = {
        subscriptionToken: null 
    },
    h = {},
    i = {
        WRAPPER_JS_CLASS: ".js-form-steps",
        SINGLE_STEP_JS_CLASS: ".js-form-step",
        COUNTER_JS_CLASS: ".js-form-step-counter",
        COUNTER_ITEM_JS_CLASS: ".js-form-step-counter-item",
        AFTER_FINAL_STEP_JS_CLASS: ".js-form-step-after-final",
        ACTIVE_STEP_CSS_CLASS: "form-step-active",
        INACTIVE_STEP_CSS_CLASS: "form-step-inactive",
        ACTIVE_COUNTER_ITEM_CSS_CLASS: "active",
        ACTIVATE_ANIMATION_CSS_CLASS: "fadeIn"
    },
    d.init = function(a) {
        this.$wrapper = a,
        this.$steps = this.$wrapper.find(i.SINGLE_STEP_JS_CLASS),
        this.$afterFinalStepScreen = this.$wrapper.find(i.AFTER_FINAL_STEP_JS_CLASS),
        this.$counter = this.$wrapper.find(i.COUNTER_JS_CLASS),
        this.$counterDots = this.$counter.find(i.COUNTER_ITEM_JS_CLASS)
    }
    ,
    d.$findWrapper = function(a) {
        return a.find(i.WRAPPER_JS_CLASS)
    }
    ,
    d.$findStepParent = function(a) {
        return a.parents(i.SINGLE_STEP_JS_CLASS)
    }
    ,
    d.$findNextStep = function(a) {
        return a.next(i.SINGLE_STEP_JS_CLASS)
    }
    ,
    d.$findSingleDot = function(a) {
        return b(this.$counterDots.get(a))
    }
    ,
    f.init = function() {
        this.updateCounter()
    }
    ,
    f.updateCounter = function() {
        e.$counter.length && (e.$counterDots.removeClass(i.ACTIVE_COUNTER_ITEM_CSS_CLASS),
        e.$findSingleDot(g.currentStep - 1).addClass(i.ACTIVE_COUNTER_ITEM_CSS_CLASS))
    }
    ,
    f.deactivate = function(a) {
        a.removeClass(i.ACTIVE_STEP_CSS_CLASS).addClass(i.INACTIVE_STEP_CSS_CLASS)
    }
    ,
    f.activate = function(a) {
        a.removeClass(i.INACTIVE_STEP_CSS_CLASS).addClass(i.ACTIVE_STEP_CSS_CLASS)
    }
    ,
    f.goToNext = function(a, b, c) {
        c = c || !1,
        this.deactivate(a),
        this.activate(b),
        c && a.fadeOut(function() {
            b.addClass("animated " + i.ACTIVATE_ANIMATION_CSS_CLASS)
        })
    }
    ,
    g.init = function() {
        this.currentStep = 1
    }
    ,
    g.setCurrentStep = function(a) {
        this.currentStep = a
    }
    ,
    h.init = function() {
        null  === g.subscriptionToken && this.subscribe()
    }
    ,
    h.initAllTheThings = function(a) {
        var b = e.$findWrapper(a);
        b.length <= 0 || (d.init(b),
        g.init(),
        f.init(),
        h.init())
    }
    ,
    h.subscribe = function() {
        g.subscriptionToken = Crew.Ajax.Form.subscribe("submitSuccess", function(a) {
            var b = e.$findStepParent(a)
              , c = e.$findNextStep(b);
            return c.length ? (f.goToNext(b, c, !0),
            g.setCurrentStep(c.data("step-id")),
            void f.updateCounter()) : void (e.$afterFinalStepScreen.length && (f.goToNext(b, e.$afterFinalStepScreen, !0),
            g.setCurrentStep(g.currentStep + 1),
            f.updateCounter()))
        })
    }
    ,
    h.unsubscribe = function() {
        null  !== g.subscriptionToken && (Crew.Ajax.Form.unsubscribe(g.subscriptionToken),
        g.subscriptionToken = null )
    }
    ,
    b(function() {
        h.initAllTheThings(b(document))
    }),
    Crew.Utils.initializers.add(function(a) {
        h.initAllTheThings(a)
    }),
    Crew.Utils.terminators.add(function(a) {
        h.unsubscribe()
    })
}(window, jQuery);
var Crew = Crew || {};
Crew.Utils = Crew.Utils || {},
function(a, b, c) {
    var d, e, f, g, h;
    d = e = {},
    f = {},
    g = {},
    h = {
        MODAL_AUTO_SHOW_JS_CLASS: ".js-modal-show-yourself"
    },
    d.init = function(a) {
        this.$modals = this.$findModals(a)
    }
    ,
    d.$findModals = function(a) {
        return a.find(h.MODAL_AUTO_SHOW_JS_CLASS)
    }
    ,
    f.init = function() {
        this.showModals()
    }
    ,
    f.showModals = function() {
        _.each(e.$modals, function(a, c) {
            b(a).modal("show")
        })
    }
    ,
    g.init = function(a) {
        e.$findModals(a).length <= 0 || (d.init(a),
        f.init())
    }
    ,
    b(function() {
        g.init(b(document))
    }),
    Crew.Utils.initializers.add(function(a) {
        g.init(a)
    })
}(window, jQuery);
var Crew = Crew || {};
Crew.Utils = Crew.Utils || {},
function(a, b, c) {
    var d, e, f, g, h;
    d = e = {},
    f = {},
    g = {},
    h = {
        ONBOARDING_MODAL_SELECTOR: ".js-network-onboarding-modal",
        BODY_CLASS: "app-minimal"
    },
    d.init = function(a) {
        this.$modal = a,
        this.$body = b("body")
    }
    ,
    d.$findModal = function(a) {
        return a.find(h.ONBOARDING_MODAL_SELECTOR)
    }
    ,
    f.removeBodyClass = function() {
        e.$body.removeClass(h.BODY_CLASS)
    }
    ,
    g.init = function(a) {
        var b = e.$findModal(a);
        b.length <= 0 || (d.init(b),
        g.listenToClosing())
    }
    ,
    g.listenToClosing = function() {
        e.$modal.off("hide.bs.modal").on("hide.bs.modal", function(a) {
            f.removeBodyClass()
        })
    }
    ,
    b(function() {
        g.init(b(document))
    }),
    Crew.Utils.initializers.add(function(a) {
        g.init(a)
    })
}(window, jQuery);
var Crew = Crew || {};
Crew.Utils = Crew.Utils || {},
Crew.Ajax = Crew.Ajax || {},
Crew.Ajax.Form = Crew.Ajax.Form || {},
function(a, b, c) {
    var d, e, f, g, h, i;
    d = e = {},
    f = {},
    g = {},
    i = {},
    h = {
        INTRODUCTION_SUBMIT_SELECTOR: ".js-refer-project-introduction",
        SUBMIT_SELECTOR: ".js-save-button",
        REFER_FORM_SELECTOR: ".js-refer-project-form",
        MODAL_SELECTOR: ".js-refer-project-introduction-form",
        INTRO_HIDDEN_FIELD_SELECTOR: "input[type=hidden][name=isIntro]"
    },
    d.init = function(a) {
        this.$submitIntro = a,
        this.$referralForm = b(h.REFER_FORM_SELECTOR),
        this.$submits = this.$referralForm.find(h.SUBMIT_SELECTOR),
        this.$modal = b(h.MODAL_SELECTOR),
        this.$introForms = this.$modal.find("form")
    }
    ,
    d.$findIntroductionSubmit = function(a) {
        return a.find(h.INTRODUCTION_SUBMIT_SELECTOR)
    }
    ,
    f.setIntroValue = function(a) {
        var d = a.data("is-intro") !== c ? a.data("is-intro") : 0;
        b(h.INTRO_HIDDEN_FIELD_SELECTOR).val(d)
    }
    ,
    f.cancelRedirection = function() {
        e.$referralForm.removeData("redirect-on-success"),
        e.$referralForm.removeAttr("data-redirect-on-success")
    }
    ,
    f.resetRedirection = function() {
        var a = e.$referralForm.data("redirect-on-success")
          , b = a !== c ? a : g.defaultRedirection;
        e.$referralForm.data("redirect-on-success", b)
    }
    ,
    f.transferRedirection = function() {
        e.$introForms.data("redirect-on-success", g.defaultRedirection)
    }
    ,
    g.init = function() {
        this.subscriptionToken = null ,
        this.currentSubmitButton = null ,
        this.defaultRedirection = null 
    }
    ,
    i.init = function(a) {
        var b = e.$findIntroductionSubmit(a);
        b.length <= 0 || (d.init(b),
        g.init(),
        i.listenToSubmit(),
        i.subscribe())
    }
    ,
    i.reset = function(a) {
        e.$findIntroductionSubmit(a).length <= 0 || (e.$submits.off("click.networkIntroduction"),
        i.unsubscribe())
    }
    ,
    i.listenToSubmit = function() {
        e.$submits.on("click.networkIntroduction", function(a) {
            g.currentSubmitButton = b(this),
            f.setIntroValue(b(this)),
            f.resetRedirection(),
            g.currentSubmitButton.is(e.$submitIntro) === !0 && (g.defaultRedirection = e.$referralForm.data("redirect-on-success"),
            f.cancelRedirection())
        })
    }
    ,
    i.subscribe = function() {
        g.subscriptionToken = Crew.Ajax.Form.subscribe("submitSuccess", function(a) {
            a.is(e.$referralForm) !== !1 && g.currentSubmitButton.is(e.$submitIntro) !== !1 && (f.transferRedirection(),
            e.$modal.modal("show"))
        })
    }
    ,
    i.unsubscribe = function() {
        null  !== g.subscriptionToken && (Crew.Ajax.Form.unsubscribe(g.subscriptionToken),
        g.subscriptionToken = null )
    }
    ,
    b(function() {
        i.init(b(document))
    }),
    Crew.Utils.initializers.add(function(a) {
        i.init(a)
    }),
    Crew.Utils.terminators.add(function(a) {
        i.reset(a)
    })
}(window, jQuery);
