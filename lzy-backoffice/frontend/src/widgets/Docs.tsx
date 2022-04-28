import overview from '../docs/0-overview.md';
import setup from '../docs/1-setup.md';
import basics from '../docs/2-basics.md';
import environment from '../docs/3-environment.md';
import cache from '../docs/4-cache.md';
import whiteboard from '../docs/5-whiteboards.md';
import views from '../docs/6-views.md';
import integrations from '../docs/7-integrations.md'
import ReactMarkdown from 'react-markdown'
import {Header} from './Header';
import {useState} from 'react';

export function Overview(props: {}) {
    let [state, setState] = useState<any>(null);
    if (state == null) {
        fetch(overview)
            .then(async (response) => {
                    let mdtext = await response.text();
                    setState(<ReactMarkdown className="markdown-body">{mdtext}</ReactMarkdown>);
                }
            )
    }
    return (
        <>
            <Header/>
            {state}
        </>
    )
}

export function Setup(props: {}) {
    let [state, setState] = useState<any>(null);
    if (state == null) {
        fetch(setup)
            .then(async (response) => {
                    let mdtext = await response.text();
                    setState(<ReactMarkdown className="markdown-body">{mdtext}</ReactMarkdown>);
                }
            )
    }
    return (
        <>
            <Header/>
            {state}
        </>
    )
}

export function Basics(props: {}) {
    let [state, setState] = useState<any>(null);
    if (state == null) {
        fetch(basics)
            .then(async (response) => {
                    let mdtext = await response.text();
                    setState(<ReactMarkdown className="markdown-body">{mdtext}</ReactMarkdown>);
                }
            )
    }
    return (
        <>
            <Header/>
            {state}
        </>
    )
}

export function Environment(props: {}) {
    let [state, setState] = useState<any>(null);
    if (state == null) {
        fetch(environment)
            .then(async (response) => {
                    let mdtext = await response.text();
                    setState(<ReactMarkdown className="markdown-body">{mdtext}</ReactMarkdown>);
                }
            )
    }
    return (
        <>
            <Header/>
            {state}
        </>
    )
}

export function Cache(props: {}) {
    let [state, setState] = useState<any>(null);
    if (state == null) {
        fetch(cache)
            .then(async (response) => {
                    let mdtext = await response.text();
                    setState(<ReactMarkdown className="markdown-body">{mdtext}</ReactMarkdown>);
                }
            )
    }
    return (
        <>
            <Header/>
            {state}
        </>
    )
}

export function Whiteboards(props: {}) {
    let [state, setState] = useState<any>(null);
    if (state == null) {
        fetch(whiteboard)
            .then(async (response) => {
                    let mdtext = await response.text();
                    setState(<ReactMarkdown className="markdown-body">{mdtext}</ReactMarkdown>);
                }
            )
    }
    return (
        <>
            <Header/>
            {state}
        </>
    )
}

export function Views(props: {}) {
    let [state, setState] = useState<any>(null);
    if (state == null) {
        fetch(views)
            .then(async (response) => {
                    let mdtext = await response.text();
                    setState(<ReactMarkdown className="markdown-body">{mdtext}</ReactMarkdown>);
                }
            )
    }
    return (
        <>
            <Header/>
            {state}
        </>
    )
}

export function Integrations(props: {}) {
    let [state, setState] = useState<any>(null);
    if (state == null) {
        fetch(integrations)
            .then(async (response) => {
                    let mdtext = await response.text();
                    setState(<ReactMarkdown className="markdown-body">{mdtext}</ReactMarkdown>);
                }
            )
    }
    return (
        <>
            <Header/>
            {state}
        </>
    )
}
